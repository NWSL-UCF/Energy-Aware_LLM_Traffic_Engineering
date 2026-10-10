"""
Topology manager for the spine-leaf fabric.

Discovers switches and links through ryu.topology, maintains the directed
switch graph used for path computation and the switch/link/port tables the
other apps read, learns host locations from ARP packet-ins (persisted under
data/access_table/), and provides the k-shortest paths between leaf switches:
loaded from data/all_paths/ at start-up, or computed with Yen's algorithm and
saved there when no path file is available. Topology events are debounced so
the event loop never blocks; path work runs in a background green thread.
"""

import glob
import json
import logging
import os
import time

import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.lib import hub
from ryu.lib.packet import packet, arp
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_all_switch, get_all_link

import setting

LOG = logging.getLogger('ryu.app.topology_manager')

# Data files of the spine-leaf fabric.
ACCESS_TABLE_EDGE_FILE = 'data/access_table/access_table_edge_spine-leaf.json'
ALL_PATHS_FILE = 'data/all_paths/all_paths_spine-leaf.json'
ALL_PATHS_DPID_FILE = 'data/all_paths/all_paths_dpid_spine-leaf.json'

# Leaf switches of the fabric: paths are kept between every ordered pair of
# them. The fabric has NUM_SWITCHES switches in total; path computation waits
# until all of them have been discovered.
LEAF_DPIDS = list(setting.LLEAF_DPIDS)
NUM_SWITCHES = len(setting.DPID_TO_NAME)


class TopologyManagerV2(app_manager.RyuApp):
    """Topology discovery, host learning and k-shortest-path provider."""
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(TopologyManagerV2, self).__init__(*args, **kwargs)

        self.k = setting.k                                  # paths per switch pair
        self.scheduler_interval = setting.scheduler_interval
        self.global_start_time = time.time()
        self.enable_logging = setting.TM_LOGGING            # per-pair path computation logs
        self.use_all_paths_start = setting.USE_ALL_PATHS_START

        # Core topology data
        self.topology = nx.DiGraph()        # directed switch graph used for path computation
        self.switches = {}                  # {dpid: ryu Switch}
        self.links = {}                     # {(src_dpid, dst_dpid): link_info}
        self.hosts = {}                     # {host_ip: (dpid, port)}

        # Paths. Other apps bind these dicts at start-up, so they are only ever
        # updated in place.
        self.all_paths = {}                 # {path_id: [[dpid, link_data], ...]}
        self.all_paths_dpid = {}            # {path_id: [dpid, ...]}

        # Port and link tables
        self.link_port_table = {}           # {(src_dpid, dst_dpid): (src_port, dst_port)}
        self.switch_all_ports_table = {}    # {dpid: set(ports)}
        self.not_use_ports = {}             # {dpid: set(ports without an inter-switch link)}

        # Host locations
        self.access_table = {}              # runtime {(dpid, port): (ip, mac)}
        self.access_table_edge = {}         # persisted {(dpid, port): (ip, mac)}
        self.access_table_edge_file = ACCESS_TABLE_EDGE_FILE
        self.all_paths_file = ALL_PATHS_FILE
        self.all_paths_dpid_file = ALL_PATHS_DPID_FILE

        if self.use_all_paths_start:
            LOG.info("Loading precomputed paths at startup")
            self.load_paths_from_json()

        self.topology_version = 0

        # Debounce for topology events
        self._topo_debounce_thread = None   # green thread handle
        self._topo_debounce_delay = 30.0    # seconds of quiet before the update runs

        self._load_access_table_edge()
        self.scheduler_thread = hub.spawn(self._scheduler)

        LOG.info(f'Topology Manager initialized. NAME: {self.name}')

    # ==================== Initialization ====================

    def _load_access_table_edge(self):
        """Load the persisted host table and populate the runtime tables"""
        try:
            with open(self.access_table_edge_file, 'r') as f:
                existing_data = json.load(f)
                self.access_table_edge = {
                    tuple(map(int, k.split(':'))): tuple(v)
                    for k, v in existing_data.items()
                }
                for (dpid, port), (ip, mac) in self.access_table_edge.items():
                    self.access_table[(dpid, port)] = (ip, mac)
                    self.hosts[ip] = (dpid, port)
                LOG.info(f"Loaded {len(self.access_table_edge)} hosts from {self.access_table_edge_file}")
        except (FileNotFoundError, json.JSONDecodeError):
            with open(self.access_table_edge_file, 'w') as f:
                json.dump({}, f)
                LOG.info(f"Created new file {self.access_table_edge_file}")

    # ==================== Event Handlers (Debounced) ====================

    @set_ev_cls(topo_event.EventSwitchEnter, MAIN_DISPATCHER)
    @set_ev_cls(topo_event.EventLinkAdd, MAIN_DISPATCHER)
    def handle_topology_add(self, ev):
        """Handle topology addition events (debounced - does zero blocking work)"""
        LOG.debug("Topology add event, scheduling debounced update")
        self._schedule_debounced_update()

    @set_ev_cls(topo_event.EventLinkDelete, MAIN_DISPATCHER)
    @set_ev_cls(topo_event.EventSwitchLeave, MAIN_DISPATCHER)
    def handle_topology_delete(self, ev):
        """Handle topology deletion events (debounced - does zero blocking work)"""
        LOG.debug("Topology delete event, scheduling debounced update")
        self._schedule_debounced_update()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Handle switch feature reply"""
        datapath = ev.msg.datapath
        self._install_default_flows(datapath)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """Handle packet-in events for ARP learning"""
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)

        if isinstance(arp_pkt, arp.arp):
            self._learn_host_location(datapath.id, in_port,
                                      arp_pkt.src_ip, arp_pkt.src_mac)

    # ==================== Debounce Logic ====================

    def _schedule_debounced_update(self):
        """Kill previous debounce timer, start a new one.

        Each topology event resets the timer. Only when events stop arriving
        for _topo_debounce_delay seconds does the actual work run.
        """
        if self._topo_debounce_thread is not None:
            hub.kill(self._topo_debounce_thread)
            self._topo_debounce_thread = None
        self._topo_debounce_thread = hub.spawn(self._debounced_topology_update)

    def _debounced_topology_update(self):
        """Runs in a background green thread after events settle: rebuilds the
        topology graph, then loads or computes the paths."""
        hub.sleep(self._topo_debounce_delay)

        LOG.info("Debounce elapsed - performing topology update")
        self._update_topology()
        self._compute_all_paths_spine_leaf()

        if self.all_paths:
            LOG.info(f"Topology READY: {len(self.switches)} switches, "
                     f"{len(self.all_paths)} paths available")

    # ==================== Core Topology Methods ====================

    def _update_topology(self):
        """Update the network topology with current switch and link information"""
        LOG.info("Updating network topology")

        self.topology.clear()
        self.topology_version += 1

        switches = get_all_switch(self)
        if not switches:
            LOG.warning("No switches found in topology")
            return

        # Rebuild into fresh dicts, then replace the contents of the existing
        # dict objects in place: other apps hold references to them.
        new_switches = {}
        new_ports = {}
        new_link_ports = {}
        new_links = {}

        for switch in switches:
            dpid = switch.dp.id
            self.topology.add_node(dpid)
            new_switches[dpid] = switch
            new_ports[dpid] = {port.port_no for port in switch.ports}

        for link in get_all_link(self):
            src_dpid, dst_dpid = link.src.dpid, link.dst.dpid
            src_port, dst_port = link.src.port_no, link.dst.port_no

            self.topology.add_edge(src_dpid, dst_dpid,
                                   src_port=src_port, dst_port=dst_port,
                                   bandwidth=setting.default_bandwidth,
                                   delay=setting.default_delay)

            new_link_ports[(src_dpid, dst_dpid)] = (src_port, dst_port)
            new_links[(src_dpid, dst_dpid)] = {
                'src_port': src_port,
                'dst_port': dst_port,
                'bandwidth': setting.default_bandwidth,
                'delay': setting.default_delay
            }

        self.switches.clear()
        self.switches.update(new_switches)
        self.switch_all_ports_table.clear()
        self.switch_all_ports_table.update(new_ports)
        self.link_port_table.clear()
        self.link_port_table.update(new_link_ports)
        self.links.clear()
        self.links.update(new_links)

        self._update_unused_ports()

        LOG.info(f"Topology updated: {len(self.switches)} switches, {len(self.links)} links")

    def _update_unused_ports(self):
        """Identify ports not used for inter-switch links (potential host ports)"""
        unused = {}
        for dpid, ports in self.switch_all_ports_table.items():
            used_ports = set()
            for (src, dst), (src_port, dst_port) in self.link_port_table.items():
                if src == dpid:
                    used_ports.add(src_port)
                elif dst == dpid:
                    used_ports.add(dst_port)
            unused[dpid] = ports - used_ports
        self.not_use_ports.clear()
        self.not_use_ports.update(unused)

    # ==================== Path Computation Methods ====================

    def _compute_all_paths_spine_leaf(self):
        """Provide the k-shortest paths between every ordered pair of leaf switches.

        With USE_ALL_PATHS_START the paths come from the JSON files and nothing
        is computed. Otherwise, or when no path file could be loaded, they are
        computed with Yen's algorithm once all NUM_SWITCHES switches are in the
        topology, and saved to data/all_paths/.
        """
        if self.use_all_paths_start:
            if not (self.all_paths and self.all_paths_dpid):
                self.load_paths_from_json()
            if self.all_paths and self.all_paths_dpid:
                LOG.info(f"Using {len(self.all_paths)} precomputed paths")
                return
            LOG.warning("No precomputed path file available, computing paths from the live topology")

        if len(self.topology.nodes) < NUM_SWITCHES:
            missing_leaves = sorted(set(LEAF_DPIDS) - set(self.topology.nodes))
            LOG.warning(f"Topology incomplete ({len(self.topology.nodes)}/{NUM_SWITCHES} switches), "
                        f"missing leaf switches: {missing_leaves}")
            return

        LOG.info(f"Computing {self.k} shortest paths between {len(LEAF_DPIDS)} leaf switches")
        self.all_paths.clear()
        self.all_paths_dpid.clear()

        for src in LEAF_DPIDS:
            for dst in LEAF_DPIDS:
                if src == dst:
                    continue
                paths = self._yens_algorithm(src, dst, self.k)
                self._store_paths(src, dst, paths)
                if self.enable_logging:
                    LOG.info(f"Computed {len(paths)} paths from {src} to {dst}")
                hub.sleep(0.5)  # yield to other green threads

        LOG.info(f"Computed paths for {len(LEAF_DPIDS)}x{len(LEAF_DPIDS) - 1} ordered leaf pairs: "
                 f"{len(self.all_paths)} paths")
        self._save_paths_to_json()

    def _yens_algorithm(self, source, target, k):
        """Yen's k-shortest paths algorithm"""
        try:
            A = [nx.shortest_path(self.topology, source, target, weight='weight')]
            B = []

            for _ in range(1, k):
                for j in range(len(A[-1]) - 1):
                    spur_node = A[-1][j]
                    root_path = A[-1][:j + 1]

                    # Temporarily remove the edges already used by paths sharing this root
                    removed_edges = []
                    for path in A:
                        if len(path) > j and root_path == path[:j + 1]:
                            edge = (path[j], path[j + 1])
                            if self.topology.has_edge(*edge):
                                edge_data = self.topology[edge[0]][edge[1]]
                                self.topology.remove_edge(*edge)
                                removed_edges.append((edge, edge_data))

                    try:
                        spur_path = nx.shortest_path(self.topology, spur_node, target, weight='weight')
                        total_path = root_path[:-1] + spur_path

                        if self._is_valid_path(total_path) and total_path not in B:
                            B.append(total_path)
                    except nx.NetworkXNoPath:
                        pass

                    for edge, data in removed_edges:
                        self.topology.add_edge(edge[0], edge[1], **data)

                if not B:
                    break

                B.sort(key=len)
                A.append(B.pop(0))

            return A

        except nx.NetworkXNoPath:
            LOG.warning(f"No path found between {source} and {target}")
            return []

    def _is_valid_path(self, path):
        """Check if path contains no loops"""
        return len(path) == len(set(path))

    def _store_paths(self, src, dst, paths):
        """Store computed paths as "<src>-<dst>-path<N>" with per-hop link information"""
        for idx, path in enumerate(paths):
            path_id = f"{src}-{dst}-path{idx + 1}"

            detailed_path = []
            for i in range(len(path) - 1):
                node = path[i]
                next_node = path[i + 1]

                if self.topology.has_edge(node, next_node):
                    link_data = self.topology[node][next_node]
                    detailed_path.append([node, link_data])
                else:
                    detailed_path.append([node, None])

            if path:
                detailed_path.append([path[-1], None])

            self.all_paths[path_id] = detailed_path
            self.all_paths_dpid[path_id] = path

    def _save_paths_to_json(self):
        """Save all_paths and all_paths_dpid to timestamped JSON files under data/all_paths/"""
        try:
            os.makedirs('data/all_paths', exist_ok=True)

            all_paths_serializable = {}
            for path_id, path_data in self.all_paths.items():
                serializable_path = []
                for node, link_data in path_data:
                    if link_data is None:
                        serializable_path.append([node, None])
                    else:
                        serializable_link = {k: v for k, v in link_data.items()
                                             if isinstance(v, (int, float, str, bool, type(None)))}
                        serializable_path.append([node, serializable_link])
                all_paths_serializable[path_id] = serializable_path

            all_paths_file = f'data/all_paths/all_paths_{int(self.global_start_time)}.json'
            with open(all_paths_file, 'w') as f:
                json.dump(all_paths_serializable, f, indent=2)

            all_paths_dpid_file = f'data/all_paths/all_paths_dpid_{int(self.global_start_time)}.json'
            with open(all_paths_dpid_file, 'w') as f:
                json.dump(self.all_paths_dpid, f, indent=2)

            LOG.info(f"Saved paths to {all_paths_file} and {all_paths_dpid_file}")

        except Exception as e:
            LOG.error(f"Error saving paths to JSON: {e}")

    def load_paths_from_json(self):
        """Load all_paths and all_paths_dpid from the JSON files.

        Falls back to the newest timestamped file of each kind when the
        configured file is missing. On failure both dicts are left empty so
        the paths get computed from the live topology.
        """
        try:
            all_paths_file = self.all_paths_file
            all_paths_dpid_file = self.all_paths_dpid_file

            if not os.path.exists(all_paths_file):
                LOG.warning(f"File {all_paths_file} not found, looking for timestamped versions")
                matching_files = glob.glob('data/all_paths/all_paths_[0-9]*.json')
                if matching_files:
                    all_paths_file = max(matching_files, key=os.path.getmtime)
                    LOG.info(f"Using timestamped file: {all_paths_file}")

            if not os.path.exists(all_paths_dpid_file):
                LOG.warning(f"File {all_paths_dpid_file} not found, looking for timestamped versions")
                matching_files = glob.glob('data/all_paths/all_paths_dpid_[0-9]*.json')
                if matching_files:
                    all_paths_dpid_file = max(matching_files, key=os.path.getmtime)
                    LOG.info(f"Using timestamped file: {all_paths_dpid_file}")

            with open(all_paths_file, 'r') as f:
                loaded_paths = json.load(f)
            with open(all_paths_dpid_file, 'r') as f:
                loaded_paths_dpid = json.load(f)

            self.all_paths.clear()
            self.all_paths.update(loaded_paths)
            self.all_paths_dpid.clear()
            self.all_paths_dpid.update(loaded_paths_dpid)

            LOG.info(f"Loaded paths from {all_paths_file} and {all_paths_dpid_file}")
            LOG.info(f"Loaded {len(self.all_paths)} detailed paths and {len(self.all_paths_dpid)} simple paths")

        except Exception as e:
            LOG.error(f"Error loading paths from JSON: {e}")
            LOG.info("Will compute paths from scratch")
            self.all_paths.clear()
            self.all_paths_dpid.clear()

    # ==================== Host Management Methods ====================

    def _learn_host_location(self, dpid, port, ip, mac):
        """Learn and store host location"""
        key = (dpid, port)

        if port in self.not_use_ports.get(dpid, set()):
            self.access_table[key] = (ip, mac)
            self.hosts[ip] = key
            self._update_edge_table(dpid, port, ip, mac)

    def _update_edge_table(self, dpid, port, ip, mac):
        """Persist a host that is new or has moved; known unchanged hosts are skipped"""
        key = (dpid, port)
        if self.access_table_edge.get(key) == (ip, mac):
            return

        for old_key, (old_ip, _) in list(self.access_table_edge.items()):
            if old_ip == ip and old_key != key:
                del self.access_table_edge[old_key]

        self.access_table_edge[key] = (ip, mac)
        self._save_edge_table()
        LOG.info(f"Updated edge table with host {ip} at {dpid}:{port}")

    def _save_edge_table(self):
        """Write the full access_table_edge to its JSON file"""
        try:
            data = {f"{dpid}:{port}": list(v) for (dpid, port), v in self.access_table_edge.items()}
            with open(self.access_table_edge_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            LOG.error(f"Failed to save edge table: {e}")

    def get_host_ip_location(self, host_ip):
        """Get the switch location (dpid, in_port) of a host by its IP, or None"""
        if host_ip in ("0.0.0.0", "255.255.255.255"):
            return None
        location = self.hosts.get(host_ip)
        if location:
            return location
        LOG.debug(f"Host IP {host_ip} location not found")
        return None

    # ==================== Switch Setup / Scheduler ====================

    def _install_default_flows(self, datapath):
        """Install the ARP and IPv4 punt-to-controller entries on a new switch"""
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch(eth_type=setting.ETH_TYPE_ARP)
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=setting.PRIORITY_ARP,
                                match=match, instructions=inst)
        datapath.send_msg(mod)

        match_ip = parser.OFPMatch(eth_type=0x0800)
        mod_ip = parser.OFPFlowMod(datapath=datapath, priority=setting.PRIORITY_IPV4,
                                   match=match_ip, instructions=inst)
        datapath.send_msg(mod_ip)

        LOG.info(f'Installed default flows on switch {datapath.id}')

    def _scheduler(self):
        """Periodically refresh the topology graph from the discovery data"""
        while True:
            LOG.info("Scheduler: Updating topology")
            self._update_topology()
            hub.sleep(self.scheduler_interval)

    # ==================== REST API Methods ====================

    def get_topology_summary(self):
        """Get topology summary statistics"""
        return {
            'switch_count': len(self.switches),
            'link_count': len(self.links),
            'host_count': len(self.hosts),
            'path_count': len(self.all_paths),
            'topology_version': self.topology_version
        }

    def get_topology_details(self):
        """Get nodes/links in the shape the topology visualization expects"""
        nodes = []
        links = []

        for dpid in self.switches:
            nodes.append({
                'id': str(dpid),
                'type': 'switch',
                'label': f'S{dpid}',
                'dpid': dpid
            })

        for ip, (dpid, port) in self.hosts.items():
            nodes.append({
                'id': ip,
                'type': 'host',
                'label': ip,
                'ip': ip,
                'connected_to': str(dpid),
                'port': port
            })

        for (src_dpid, dst_dpid), link_info in self.links.items():
            links.append({
                'id': f'{src_dpid}-{dst_dpid}',
                'from': str(src_dpid),
                'to': str(dst_dpid),
                'type': 'switch_link',
                'src_port': link_info.get('src_port'),
                'dst_port': link_info.get('dst_port'),
                'bandwidth': link_info.get('bandwidth', 0),
                'delay': link_info.get('delay', 0)
            })

        for ip, (dpid, port) in self.hosts.items():
            links.append({
                'id': f'{ip}-{dpid}',
                'from': ip,
                'to': str(dpid),
                'type': 'host_link',
                'port': port
            })

        return {'nodes': nodes, 'links': links}

    def get_path_info(self, src, dst):
        """Get every stored path from src to dst"""
        paths = []
        path_prefix = f"{src}-{dst}-path"

        for path_id, path_details in self.all_paths.items():
            if path_id.startswith(path_prefix):
                paths.append({
                    'path_id': path_id,
                    'path': path_details,
                    'hop_count': len(path_details) - 1 if path_details else 0
                })

        return paths

    # ==================== Shutdown ====================

    def close(self):
        """Called by ryu on shutdown; stop the scheduler and debounce greenlets."""
        try:
            self._cleanup()
        except Exception as e:
            LOG.error(f"[TopologyManager] cleanup error: {e}")
        super(TopologyManagerV2, self).close()

    def _cleanup(self):
        """Kill the scheduler and any pending debounce greenlet"""
        if self.scheduler_thread is not None:
            hub.kill(self.scheduler_thread)
            self.scheduler_thread = None

        if self._topo_debounce_thread is not None:
            hub.kill(self._topo_debounce_thread)
            self._topo_debounce_thread = None

        LOG.info("Topology Manager cleaned up")
