"""Link utilization and per-cycle network energy aggregates.

Every LINK_MONITOR_INTERVAL seconds the monitor polls port stats from every
registered (non-excluded) switch, turns the tx_bytes delta of each port into a
bounded directed-link utilization, and once per cycle publishes an immutable
power_model.EnergySnapshot (network totals + per-path (P_norm, ALU, MLU,
Delay_Rank)) together with the network power row that is appended to
data/power/power_<ts>.jsonl.  Published objects are rebound, never mutated, so
readers in other apps always see a consistent cycle.
"""
import time
from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_3
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.base.app_manager import lookup_service_brick
from ryu.lib import hub
from events import EventDelayUpdate

import setting
import power_model
import json
import os
from datetime import datetime
from eventlet import queue as eventlet_queue


TOPOLOGY_STATS_FILE = 'data/topology_stats/topology_stats_spine-leaf.json'


def bounded_utilization(delta_bytes, capacity_bps, dt_s, fallback_dt_s):
    """Bounded link utilization over the MEASURED sampling gap.

    util = min(setting.UTILIZATION_MAX, max(0.0, delta_bytes * 8 / (capacity_bps * dt)))
    where dt = dt_s if dt_s > 0 else fallback_dt_s (a missing / non-positive gap
    falls back to the constant poll interval).  capacity_bps <= 0 -> 0.0.
    """
    dt = dt_s if (dt_s is not None and dt_s > 0) else fallback_dt_s
    if dt is None or dt <= 0 or capacity_bps is None or capacity_bps <= 0:
        return 0.0
    util = (float(delta_bytes) * 8.0) / (float(capacity_bps) * float(dt))
    return min(float(setting.UTILIZATION_MAX), max(0.0, util))


class NetworkMonitor(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(NetworkMonitor, self).__init__(*args, **kwargs)
        self.name = 'NetworkMonitor'
        self.discovery = lookup_service_brick('TopologyManagerV2')
        # Every link is treated as LINK_MAX_CAPACITY bps when converting bytes to utilization.
        self.DEFAULT_LINK_CAPACITY = setting.LINK_MAX_CAPACITY

        self.start_time = self.discovery.global_start_time

        # {path_id: [[dpid, link_data | None], ...]} from the topology manager
        self.path_db = self.discovery.all_paths

        # {src_dpid: {dst_dpid: delay_s}} from the delay detector
        self.delay = {}

        # {dpid: datapath}
        self.datapaths_table = {}
        # {pathID: (path_power_norm, ALU, MLU, Delay_Rank)}
        # Published by REBINDING once per cycle (never mutated in place) so readers
        # holding the previous dict see a consistent view.
        self.Agent1_PathStats = {}
        # Fixed set of directed link keys ("s{src}:s{dst}") the monitor reports; the
        # denominator of every network-wide energy aggregate (720 in spine-leaf).
        self.monitored_link_keys = frozenset()
        # power_model.EnergySnapshot of the last cycle (rebound, never mutated)
        self.energy_snapshot = None
        self._energy_cycle = 0
        # Network power log (data/power/power_<ts>.jsonl): ts of the previous collection and
        # the running integral of P_total over this run; power_row = last row (REBOUND per cycle).
        self._power_prev_ts = None
        self._power_energy_cum = 0.0
        self.power_row = None

        # Directed link utilization: {"s{src}:s{dst}": util} and {"{dpid}:{port_no}": util}
        self.link_utilization_links = {}
        self.link_utilization_ports = {}

        # {"Name:portN": neighbour switch name} from the topology stats file
        self.port_link_mapping = {}
        # Reverse mapping: topology name -> dpid (e.g., "ULeaf1" -> 101)
        self.name_to_dpid = {v: k for k, v in setting.DPID_TO_NAME.items()}

        # Exclusion lists for selective monitoring
        self.excluded_switch_dpids = set()   # Switches to skip entirely (no stats request, all ports = 0)
        self.excluded_ports = set()          # (dpid, port_no) pairs to force utilization = 0

        if setting.EXCLUDE_LLEAF_MONITORING:
            # LLeaf switches: skip stats requests, all utilization = 0
            self.excluded_switch_dpids = set(setting.LLEAF_DPIDS)
            # ULeaf downlink port to its LLeaf: always report utilization = 0
            self.excluded_ports = {(dpid, setting.ULEAF_DOWNLINK_PORT)
                                   for dpid in setting.ULEAF_DPIDS}

        # {(dpid, port_no): (tx_bytes, sample_ts)} - last port-stats sample per port
        self.previous_tx_bytes = {}
        self.timestamp = datetime.now().strftime("%m-%d_%H-%M-%S")

        # Async write queue: only _writer_worker touches the disk; the monitor greenlets just enqueue rows
        self.write_queue = eventlet_queue.Queue(maxsize=100)
        self._writer_thread = hub.spawn(self._writer_worker)

        # Persistent path stats thread
        self._path_stats_thread = hub.spawn(self._path_stats_loop)

        # Periodic port-stats requests
        self._monitor_link_utilization_thread = hub.spawn(self._auto_request_handler)

        self._init_topology_capacities()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change_handler(self, ev):
        """Register or unregister datapaths on state changes."""
        datapath = ev.datapath
        dpid = datapath.id

        if ev.state == MAIN_DISPATCHER:
            if dpid not in self.datapaths_table:
                self.logger.info("Registering datapath: %016x", dpid)
                self.datapaths_table[dpid] = datapath
        elif ev.state == DEAD_DISPATCHER:
            if dpid in self.datapaths_table:
                self.logger.info("Unregistering datapath: %016x", dpid)
                del self.datapaths_table[dpid]

    def _request_port_stats(self, datapath):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

    def get_path_stats(self, pathID):
        """(P_norm, ALU, MLU, Delay_Rank) of one path, or None for an unknown id."""
        return self.Agent1_PathStats.get(pathID)

    def get_path_stats_all(self):
        return self.Agent1_PathStats

    def _auto_request_handler(self):
        """Runs in background, sending periodic port-stats requests to all datapaths."""
        next_execution_time = self.start_time + setting.LINK_MONITOR_INTERVAL
        while True:
            for dpid, datapath in self.datapaths_table.items():
                if dpid not in self.excluded_switch_dpids:
                    self._request_port_stats(datapath)
            current_time = time.time()
            # Sleep until the next interval boundary; advance the boundary first so the
            # schedule stays aligned even when the requests themselves take time.
            sleep_duration = max(0, next_execution_time - current_time)
            next_execution_time += setting.LINK_MONITOR_INTERVAL
            hub.sleep(sleep_duration)

    def _directed_link_key(self, dpid, port_no):
        """"s{dpid}:s{dst_dpid}" for the link leaving (dpid, port_no), or None when the port
        is not an inter-switch link of the topology stats file."""
        switch_name = setting.DPID_TO_NAME.get(dpid, f"s{dpid}")
        dst_name = self.port_link_mapping.get(f"{switch_name}:port{port_no}")
        if dst_name is None:
            return None
        dst_dpid = self.name_to_dpid.get(dst_name)
        if dst_dpid is None:
            return None
        return f"s{dpid}:s{dst_dpid}"

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """Handle port stats replies to calculate directed link utilization."""
        datapath = ev.msg.datapath
        dpid = datapath.id
        stats_interval = setting.LINK_MONITOR_INTERVAL  # Fallback when no measured gap is available
        now = time.time()

        # Skip excluded switches entirely (shouldn't receive replies, but guard)
        if dpid in self.excluded_switch_dpids:
            return

        for stat in ev.msg.body:
            port_no = stat.port_no
            # Skip special ports
            if port_no >= datapath.ofproto.OFPP_MAX:
                continue

            # Force excluded ports to 0 utilization
            if (dpid, port_no) in self.excluded_ports:
                self.link_utilization_ports[f"{dpid}:{port_no}"] = 0
                link_key = self._directed_link_key(dpid, port_no)
                if link_key is not None:
                    self.link_utilization_links[link_key] = 0
                continue

            tx_now = stat.tx_bytes

            # Retrieve the previous (tx_bytes, ts) sample and store the new one
            prev_sample = self.previous_tx_bytes.get((dpid, port_no))
            self.previous_tx_bytes[(dpid, port_no)] = (tx_now, now)
            if prev_sample is None:
                # First time seeing this port, can't calculate utilization yet
                continue
            tx_old, prev_ts = prev_sample
            if tx_old == 0:
                # Previous sample had no traffic counter yet, wait for the next one
                continue

            # Measured gap between the two replies (fallback: poll interval)
            dt = now - prev_ts

            transfer_data = tx_now - tx_old if tx_now >= tx_old else 0

            # Bounded to [0, UTILIZATION_MAX] over the measured gap dt
            utilization = bounded_utilization(transfer_data, self.DEFAULT_LINK_CAPACITY,
                                              dt, stats_interval)

            self.link_utilization_ports[f"{dpid}:{port_no}"] = utilization
            link_key = self._directed_link_key(dpid, port_no)
            if link_key is not None:
                self.link_utilization_links[link_key] = utilization

    def _init_topology_capacities(self):
        """Load the port -> neighbour mapping of the spine-leaf topology, pre-populate the
        excluded links with 0 utilization and fix the monitored link set."""
        with open(TOPOLOGY_STATS_FILE, 'r') as f:
            data = json.load(f)

        for port_name, port_info in data["ports"].items():
            splitted = port_name.split("-")
            port_label = f"{splitted[0]}:{splitted[1]}"
            self.port_link_mapping[port_label] = port_info["dst-dpid"]

        # Pre-populate 0 utilization for excluded switches and ports
        if setting.EXCLUDE_LLEAF_MONITORING:
            for port_label, dst_name in self.port_link_mapping.items():
                switch_name = port_label.split(":")[0]
                dst_dpid_num = self.name_to_dpid.get(dst_name)
                src_dpid_num = self.name_to_dpid.get(switch_name)
                if not src_dpid_num or not dst_dpid_num:
                    continue
                # LLeaf switch ports -> 0
                if src_dpid_num in self.excluded_switch_dpids:
                    self.link_utilization_links[f"s{src_dpid_num}:s{dst_dpid_num}"] = 0
                # ULeaf -> LLeaf downlink ports -> 0
                port_num_str = port_label.split(":")[1].replace("port", "")
                if (src_dpid_num, int(port_num_str)) in self.excluded_ports:
                    self.link_utilization_links[f"s{src_dpid_num}:s{dst_dpid_num}"] = 0

        # Fixed monitored link set: the denominator of every network-wide energy
        # aggregate (P_total, ALU_net, P_max). Rebound once; never mutated.
        self.monitored_link_keys = power_model.monitored_link_keys(
            self.port_link_mapping, self.name_to_dpid,
            self.excluded_switch_dpids, self.excluded_ports)
        self.logger.info("[NetworkMonitor] monitored link keys: %d (P_max=%.1f)",
                         len(self.monitored_link_keys),
                         len(self.monitored_link_keys) * power_model.link_power_max())

    # ==================== Async Write Queue ====================

    def _writer_worker(self):
        """Background thread: drains write_queue and appends the power rows to disk."""
        while True:
            try:
                item = self.write_queue.get()
                if item is None:
                    break
                if item['type'] == 'power':
                    self._write_power_file(item['data'])
            except Exception as e:
                self.logger.error(f"[NetworkMonitor] Writer error: {e}")

    def _write_power_file(self, data):
        """Append one network power row to data/power/power_<ts>.jsonl (writer thread only).
        One ~170-byte line per monitor cycle: append-only, no rewrite, no rotation needed."""
        os.makedirs('data/power', exist_ok=True)
        full_path = f'data/power/power_{self.timestamp}.jsonl'
        try:
            with open(full_path, 'a') as f:
                json.dump(data, f, separators=(',', ':'))
                f.write('\n')
        except Exception as e:
            self.logger.error(f"Error saving power row: {e}")

    # ==================== Persistent Path Stats Thread ====================

    def _path_stats_loop(self):
        """Persistent thread: one energy cycle every LINK_MONITOR_INTERVAL seconds."""
        while True:
            hub.sleep(setting.LINK_MONITOR_INTERVAL)
            try:
                self._calculate_path_stats()
            except Exception as e:
                self.logger.error(f"[NetworkMonitor] Path stats error: {e}")

    def _calculate_path_stats(self):
        """One energy cycle: network aggregates + per-path (P_norm, ALU, MLU, Delay_Rank).

        net   = power_model.network_energy(util, monitored, ts)   # P_total, ALU_net, MLU_net, P_max
        pe_p  = power_model.path_energy(dpids_p, util, monitored, delay_rank_p)
        state = (round(P_norm_p, 4), round(ALU_p, 4), round(MLU_p, 4), delay_rank_p)
        Results are PUBLISHED BY REBINDING (energy_snapshot, Agent1_PathStats); the
        previous objects are never mutated, so concurrent readers stay consistent.
        The network power row (NetworkEnergy fields + cycle, p_norm, dt_s, energy_cum) is
        rebound to power_row and enqueued for data/power/power_<ts>.jsonl; this greenlet
        never does file I/O itself.
        """
        util = self.link_utilization_links.copy()
        temp_delay = self.delay.copy()
        monitored = self.monitored_link_keys
        ts = time.time()

        net = power_model.network_energy(util, monitored, ts)
        states = {}
        paths = {}

        for i, (pathID, path1) in enumerate(self.path_db.items()):
            dpids = [n[0] for n in path1]
            Delay = 0
            for j in range(len(dpids) - 1):
                if dpids[j] in temp_delay:
                    if dpids[j + 1] in temp_delay[dpids[j]]:
                        Delay += temp_delay[dpids[j]][dpids[j + 1]]
            dr = self._delay_rank(Delay)
            pe = power_model.path_energy(dpids, util, monitored, dr)
            paths[pathID] = pe
            states[pathID] = (round(power_model.path_power_norm(pe), 4),
                              round(pe.alu, 4), round(pe.mlu, 4), dr)
            if (i & 0x3FF) == 0x3FF:
                hub.sleep(0)  # yield every 1024 paths

        # Publish by rebinding (never mutate the objects readers may hold)
        self._energy_cycle += 1
        self.energy_snapshot = power_model.make_snapshot(self._energy_cycle, ts, net, paths, states)
        self.Agent1_PathStats = dict(states)

        # Network power row: dt since the previous collection and the running integral
        # sum(P_total * dt) for this run; _writer_worker does the file I/O.
        prev_ts = self._power_prev_ts
        dt_s = max(0.0, ts - prev_ts) if prev_ts is not None else 0.0
        self._power_energy_cum += net.p_total * dt_s
        self._power_prev_ts = ts
        row = dict(net._asdict(), cycle=self._energy_cycle,
                   p_norm=(round(net.p_total / net.p_max, 6) if net.p_max > 0 else 0.0),
                   dt_s=round(dt_s, 3), energy_cum=round(self._power_energy_cum, 3))
        self.power_row = row                              # REBIND (REST reads it)
        if setting.ENABLE_POWER_LOG:
            try:
                self.write_queue.put_nowait({'type': 'power', 'data': row})
            except eventlet_queue.Full:
                self.logger.warning("[NetworkMonitor] Write queue full, dropping power row")

    def get_energy_snapshot(self):
        """Latest power_model.EnergySnapshot (None before the first cycle). Read-only; callers copy."""
        return self.energy_snapshot

    def get_network_stats(self):
        """The latest network power row (what data/power/power_<ts>.jsonl carries: the
        NetworkEnergy fields + cycle, p_norm, dt_s, energy_cum), or None before the first
        cycle."""
        row = self.power_row
        return dict(row) if row is not None else None

    @set_ev_cls(EventDelayUpdate)
    def _handle_delay_update(self, ev):
        """Handle delay table updates from delay detector"""
        self.delay = ev.delay_table
        self.logger.debug(f"Received delay table update: {self.delay}")

    def _delay_rank(self, delay):
        """Map a path delay in seconds to the rank used in the Agent1 state (0, 0.3, 0.6, 1)."""
        delay = delay * 1000  # Convert to milliseconds
        if delay < 30:
            return 0
        elif delay < 60:
            return 0.3
        elif delay < 100:
            return 0.6
        return 1

    def close(self):
        """ryu calls close() on Ctrl+C; run the cleanup so the writer greenlet stops."""
        try:
            self._cleanup()
        except Exception as e:
            self.logger.error(f"[NetworkMonitor] cleanup error: {e}")
        super(NetworkMonitor, self).close()

    def _cleanup(self):
        """Clear the data structures and stop the greenlets on Ctrl+C."""
        self.link_utilization_links.clear()
        self.link_utilization_ports.clear()
        self.port_link_mapping.clear()
        self.previous_tx_bytes.clear()
        self.datapaths_table.clear()
        # Rebind (never mutate) the published objects
        self.Agent1_PathStats = {}
        self.energy_snapshot = None
        self._energy_cycle = 0
        self._power_prev_ts = None
        self._power_energy_cum = 0.0
        self.power_row = None
        self.monitored_link_keys = frozenset()
        self.delay.clear()
        self.logger.info("Cleanup complete")
        # Stop threads
        self.write_queue.put(None)  # Sentinel to stop writer
        hub.sleep(0.2)              # let the writer drain what is queued (last power row)
        hub.kill(self._monitor_link_utilization_thread)
        hub.kill(self._path_stats_thread)
        hub.kill(self._writer_thread)
        self.logger.info("All threads stopped")
