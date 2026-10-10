"""
Packet-in forwarding for the energy-aware SDN controller.

Every IPv4 packet-in is parsed once on a worker greenlet, classified (MPTCP
control packet, plain TCP or UDP), given a path and installed along it with
OpenFlow 1.3 FlowMods. Plain-TCP flows and MPTCP subflows carry a cookie and,
at their ingress switch, a per-flow meter sized from the per-path bandwidth
limit that Agent1 (a PPO agent over the k-shortest paths) recomputes every
cycle from the network monitor's energy snapshot. A FlowRemoved returns the
meter to the idle pool, drops the flow-ledger entry and appends the per-flow
record to data/flows/.
"""
import random
import time
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER, DEAD_DISPATCHER
from ryu.lib.packet import packet, ethernet, ipv4, tcp, udp
from ryu.ofproto import ofproto_v1_3
import logging
from ryu.base.app_manager import lookup_service_brick
import hashlib
import setting
from packet_in_worker_pool import PacketInWorkerPool
from ryu.lib import hub
import os
import json
from collections import OrderedDict
from eventlet import queue as eventlet_queue
import bw_limit
import energy_reward
from meter_manager import MeterManager
from RL.agents.ppo_agent import PPOAgent
from RL.config import get_config


LOG = logging.getLogger('ryu.app.forwarding')

AGENT1_MODEL_PATH = "models/ppo_agent1_model_final.pt"


class Forwarding(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Forwarding, self).__init__(*args, **kwargs)
        self.discovery = lookup_service_brick('TopologyManagerV2')
        self.monitor = lookup_service_brick('NetworkMonitor')
        self.file_timestamp = time.strftime("%m-%d_%H-%M-%S")
        random.seed(42)                    # reproducible path picks
        self.agent1_logging = setting.AGENT1_LOGGING
        self.agent1_timestamp = -1         # time of the last probability update (REST)

        # Path database shared with the topology manager: {path_id: [[dpid, link_data | None], ...]}
        self.path_db = self.discovery.all_paths

        # MPTCP connections keyed by token. LRU-capped OrderedDict (MPTCP_FLOWS_MAX):
        # the newest/most-recently-touched stays at the end, the oldest is evicted
        # when the cap is exceeded, which bounds memory. SINGLE WRITER: the worker
        # pool's serial MPTCP greenlet (_process_mptcp_worker / _cleanup_mptcp_state);
        # REST readers iterate list() copies, and no writer yields mid-update.
        self.mptcp_flows = OrderedDict()  # token -> {subflow_id: details}
        # Reverse index for O(1) MP_JOIN correlation: (client_host_id, client_port) -> token.
        # Lets a toward-initiator MP_JOIN (whose token we can't resolve directly) find its
        # connection by destination client endpoint. Maintained in lockstep with mptcp_flows.
        self.mptcp_client_ep_index = {}
        self.mptcp_token_ep = {}  # token -> client_ep_key, so eviction can purge the index in O(1)

        # Agent1 output: path_key -> {'probability', 'path_stats', 'path'}; rebound once per cycle
        self.Path_db_probability = {}

        # Cookie counters (see get_cookie_ID / get_tcp_cookie_ID)
        self.mptcp_flows_cookie = setting.COOKIE_ID  # counter for MPTCP subflow cookies (MPTCP_COOKIE_BASE | ctr)
        self.mptcp_flows_cookie_max = setting.MAX_COOKIE_ID  # 29-bit counter field shared by both dynamic spaces
        self.tcp_flows_cookie = setting.COOKIE_ID    # counter for plain-TCP dynamic-flow cookies (TCP_COOKIE_BASE | ctr)

        self.agent1_reward = {}  # path_key -> reward of the last Agent1 cycle (REST)

        # ---------------- Energy-aware BW limiting state ----------------
        # Flow ledger: cookie -> {'path_key','direction','ingress_dpid','kind','bw_limit','ts'}.
        # SINGLE WRITER: the worker pool's agent-report greenlet (_handle_agent_report ->
        # ledger_add / process_flow_removed). Readers take .get()/dict() copies; under
        # eventlet there is no yield between a read and its use, so no lock is needed.
        self.flow_ledger = {}
        self.path_active_flows = {}        # path_key -> live FORWARD entries (n_active for the BW split)
        self.path_active_flows_rev = {}    # path_key -> live REVERSE entries
        self.path_activity_ts_fwd = {}     # path_key -> last install/removal ts (forward only; reward coverage)
        self.path_bw_limit = {}            # path_key -> Mbps; REBOUND once per Agent1 cycle, never mutated
        self.bw_limit_stats = {}           # summary of the last per-cycle limit table (REST)
        self.bw_cfg = bw_limit.config_from_setting()
        self._reverse_path_key_cache = {}  # forward path_key -> reverse path_key (pure function of path_db)
        self.pair_top_paths = {}           # (src_dpid, dst_dpid) -> top-N [(key, path, prob)]; REBOUND per Agent1 cycle
        self._shutting_down = False        # set by _cleanup: packet_in_handler drops everything after it
        self._reverse_cache_path_db_len = 0
        self.meter_manager = None
        if setting.ENABLE_BW_LIMIT:
            self.meter_manager = MeterManager(
                max_meter_id=setting.MAX_METER_ID,
                burst_fraction=setting.METER_BURST_FRACTION,
                logger=LOG,
                clean_on_join=setting.CLEAN_METERS_ON_JOIN,
                error_backoff_s=setting.METER_ERROR_BACKOFF_S,
                dry_run=setting.BW_LIMIT_DRY_RUN)
            LOG.info(f"[BW] meters enabled (dry_run={setting.BW_LIMIT_DRY_RUN}, "
                     f"range {setting.BW_MIN_Mbps}-{setting.BW_MAX_Mbps} Mbps, "
                     f"normalization={setting.BW_PROB_NORMALIZATION})")
        self.lifecycle_stats = {
            'flow_removed_events': 0, 'flow_removed_ignored': 0, 'flow_removed_processed': 0,
            'flow_removed_by_reason': {}, 'meter_features_replies': 0, 'installs_metered': 0,
            'ledger_dropped_dead_dpid': 0,
            'installs_unmetered': 0, 'writer_dropped': 0, 'writer_errors': 0,
        }
        self.of_error_counts = {}          # (err_type, err_code) -> count
        self._of_error_last_log = {}       # (err_type, err_code) -> last log ts
        self._warned_once = set()
        self._last_energy_cycle = None
        self._empty_reward_cycles = 0
        self.agent1_reward_terms = {}      # path_key -> term breakdown of the last cycle (REST/JSONL)
        self.agent1_cycle_count = 0
        # Energy JSONL writer: ONE greenlet does the blocking file I/O; producers put_nowait.
        self._energy_writer_q = eventlet_queue.Queue(maxsize=8192)
        self._energy_writer_paths = {}     # stream -> current file path
        self._energy_writer_disabled = set()
        self._energy_writer_thread = None

        # ---------------- Agent1 (PPO path selection) ----------------
        self.use_agent1 = setting.USE_AGENT1  # False -> shortest path (path1) for everything
        self.agent1_ready = False             # True once select_action has produced probabilities
        self.agent1_path_stats = {}           # private copy of the latest snapshot's states, set each cycle
        self.agent1_path_probabilities = {}   # path_key -> probability, sorted descending

        config = get_config()
        self.agent1 = PPOAgent(config)
        LOG.info(f"Agent device: {self.agent1.device}")

        # Checkpoint: AGENT1_MODEL_PATH is loaded at start when AGENT1_NEW_MODEL is False
        # and rewritten by every save (every 5 minutes, and on shutdown once Agent1 has run,
        # when AGENT1_SAVE_MODEL).
        self._last_model_save = time.time()
        model_path = AGENT1_MODEL_PATH
        if not setting.AGENT1_NEW_MODEL:
            if os.path.exists(model_path):
                try:
                    self.agent1.load_model(model_path)
                    LOG.info(f"Loaded pre-trained PPO model from {model_path}")
                except Exception as e:
                    LOG.warning(f"Could not load pre-trained model: {e}. Starting with a new model.")
            else:
                LOG.info(f"No pre-trained model found at {model_path}. Starting with a new model.")
        else:
            LOG.info("AGENT1_NEW_MODEL is True. Skipping loading of pre-trained model and starting with a new one.")

        self.path_prob_thread = None
        if self.use_agent1:
            self.path_prob_thread = hub.spawn(self._path_probability_updater)

        # Worker pool for packet-in processing (parsing, path pick, install, ledger reports)
        self.worker_pool = PacketInWorkerPool(self, num_workers=setting.WORKER_POOL_SIZE)
        self.worker_pool.start()
        LOG.info(f"[Forwarding] Worker pool started with {setting.WORKER_POOL_SIZE} workers")

        if setting.ENABLE_ENERGY_JSONL or setting.FLOW_REMOVED_LOG_ENABLED:
            self._energy_writer_thread = hub.spawn(self._energy_writer)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """
        Thin dispatcher: minimal work on the Ryu event loop.
        Quick filter then enqueue to worker pool. Returns immediately.
        """
        if self._shutting_down:
            return
        # Raw-byte filter, NO packet parse on the event loop: keep IPv4 (optionally
        # 802.1Q-tagged) unicast only. LLDP / ARP / IPv6 cost one slice compare here.
        data = ev.msg.data
        et = data[12:14]
        if et == b'\x08\x00':
            off = 14
        elif et == b'\x81\x00' and data[16:18] == b'\x08\x00':
            off = 18
        else:
            return
        if len(data) < off + 20:
            return
        if data[off + 12:off + 16] == b'\x00\x00\x00\x00' or data[off + 16:off + 20] == b'\xe0\x00\x00\x16':
            return                                    # 0.0.0.0 source / 224.0.0.22 (IGMP)

        # Dispatch to worker pool (non-blocking)
        self.worker_pool.dispatch(ev)

    def _enqueue_track_cookie(self, cookie, src_dpid, path_key):
        """
        Non-blocking: hand an installed MPTCP subflow to the agent-report greenlet,
        the single writer of the flow ledger.
        """
        bw = self._bw_limit_for(path_key)
        self.worker_pool.report_to_agent({
            'type': 'mptcp_track_cookie',
            'cookie': cookie,
            'src_dpid': src_dpid,
            'path_key': path_key,
            'direction': 'fwd',
            'kind': 'mptcp',
            'bw_limit': bw,
            'timestamp': time.time(),
        })

    def _process_packet_in_worker(self, ev, pool):
        """
        Process packet_in in a worker green thread.

        Order:
        1. Parse the packet (ethernet / ipv4 / tcp|udp).
        2. Detect MPTCP once (option.kind == 30) and capture the matched option.
        3. Resolve src/dst switches.
        4. Duplicate-install guard (one packet-out for a recently installed 5-tuple).
        5. Path selection (top-N uniform random cache).
        6. install_flow with PRIORITY_TCP for every packet, including MPTCP.
        7. Report to agent thread (non-blocking).
        8. If MPTCP, hand the already-parsed payload to the serial MPTCP worker.
        """
        msg = ev.msg
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        ipv4_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        udp_pkt = pkt.get_protocol(udp.udp)

        src_ip = ipv4_pkt.src
        dst_ip = ipv4_pkt.dst
        src_port = tcp_pkt.src_port if tcp_pkt else (udp_pkt.src_port if udp_pkt else None)
        dst_port = tcp_pkt.dst_port if tcp_pkt else (udp_pkt.dst_port if udp_pkt else None)

        # --- Step 2: Detect MPTCP and capture the option object ---
        is_mptcp = False
        mptcp_option = None
        if tcp_pkt and tcp_pkt.option:
            for option in tcp_pkt.option:
                if option.kind == 30:
                    is_mptcp = True
                    mptcp_option = option
                    break

        # --- Step 3: Determine switches ---
        src_dpid, dst_dpid = self.get_switches(src_ip, dst_ip)
        if not src_dpid or not dst_dpid:
            LOG.debug("Could not determine switch locations for %s -> %s", src_ip, dst_ip)
            return

        # --- Step 4: duplicate-install guard ---
        # A punt for a 5-tuple installed < INSTALL_DEDUP_WINDOW_S ago (a retransmitted SYN
        # racing its FlowMods, or a second worker holding the same flow) is forwarded with
        # ONE packet-out on the recorded ingress/out_port and never re-installed: a second
        # install would silently replace the entry and orphan its meter.
        dedup = getattr(pool, 'dedup', None)
        dedup_key = None
        if dedup is not None:
            ip_proto = 6 if tcp_pkt is not None else (17 if udp_pkt is not None else 0)
            dedup_key = (src_ip, dst_ip, ip_proto, src_port, dst_port)
            hit = dedup.lookup(dedup_key)
            if hit is not None and hit[1] == msg.datapath.id:
                dp = self.monitor.datapaths_table.get(hit[1])
                if dp is not None and hit[2] is not None:
                    self.send_packet_out(dp, msg.buffer_id, in_port, hit[2], msg.data)
                return

        # --- Step 5: Path selection (top-N uniform random, cached) ---
        path_key = None
        path = None
        strategy = 'agent1' if self.use_agent1 else 'spf'

        cached = pool.path_cache.get(src_dpid, dst_dpid, strategy)
        if cached:
            path_key, path = cached
        elif strategy == 'agent1':
            top_paths = self._get_path_agent1_fast(src_dpid, dst_dpid)
            if top_paths:
                pool.path_cache.put(src_dpid, dst_dpid, top_paths, strategy)
                # Uniform random pick from the top-N set (same as PathCache.get)
                chosen = random.choice(top_paths)
                path_key, path = chosen[0], chosen[1]
        else:
            path_key, path = self.get_path(src_dpid, dst_dpid)
            if path_key and path:
                pool.path_cache.put(src_dpid, dst_dpid, [(path_key, path, 1.0)], strategy)

        if not path:
            LOG.debug("No path found between dpid %s and %s", src_dpid, dst_dpid)
            return

        # --- Step 6: Install the flow (three cases; see the cookie constants in setting.py) ---
        #  * MPTCP packet : PRIORITY_TCP pre-install - no cookie, no meter, unidirectional -
        #                   so traffic keeps flowing while the serial MPTCP worker installs
        #                   the PRIORITY_MPTCP subflow entry.
        #  * plain TCP    : one TCP-tagged cookie per direction, meter at each direction's
        #                   ingress (path[0] fwd, path[-1] rev), bidirectional.
        #  * UDP / other  : correct match (ip_proto 17 for UDP), no cookie, no meter.
        now = time.time()
        if dedup is not None:
            # Remember the install BEFORE sending it: a second worker holding the same
            # tuple sees the entry while this one may be parked in send_msg.
            dedup.record(dedup_key, path[0], self._ingress_out_port(path, dst_ip), now)
        if is_mptcp:
            installed = self.install_flow(path, eth.ethertype, src_ip, dst_ip, src_port, dst_port,
                                          in_port, msg.buffer_id, msg.data, setting.PRIORITY_TCP)
            if not installed and dedup is not None:
                dedup.forget(dedup_key)
        elif tcp_pkt is not None:
            cookie = self.get_tcp_cookie_ID()
            rev_cookie = self.get_tcp_cookie_ID()
            limit = self._bw_limit_for(path_key)
            rev_key = self._reverse_path_key(path_key, path) or path_key
            rev_limit = limit if setting.BW_LIMIT_REVERSE_DIRECTION else None
            rev_dedup_key = None
            if dedup is not None:
                # The reverse entry is installed at path[-1] together with the forward one.
                # If the server's SYN-ACK reaches that switch before the reverse FlowMod has
                # landed, it punts as (server -> client): without this record the worker
                # would install a second flow whose ingress entry silently REPLACES our
                # reverse entry (no FlowRemoved, meter stuck busy). Answer it with a
                # packet-out instead.
                rev_dedup_key = (dst_ip, src_ip, 6, dst_port, src_port)
                dedup.record(rev_dedup_key, path[-1], self._egress_out_port_rev(path, src_ip), now)
            installed = self.install_flow(path, eth.ethertype, src_ip, dst_ip, src_port, dst_port,
                                          in_port, msg.buffer_id, msg.data, setting.PRIORITY_TCP,
                                          cookie=cookie, bidirectional=True, limit=limit,
                                          reverse_cookie=rev_cookie, reverse_limit=rev_limit)
            if not installed and dedup is not None:
                dedup.forget(dedup_key)
                dedup.forget(rev_dedup_key)

            # --- Step 7: ledger reports (the agent-report greenlet is the single writer) ---
            # Only for flows that were really installed: an early exit in install_flow must
            # not leave phantom ledger entries (they would inflate n_active for the path).
            if installed:
                pool.report_to_agent({
                    'type': 'flow_installed', 'path_key': path_key, 'cookie': cookie,
                    'src_dpid': path[0], 'direction': 'fwd', 'kind': 'tcp',
                    'bw_limit': limit, 'timestamp': now,
                })
                pool.report_to_agent({
                    'type': 'flow_installed', 'path_key': rev_key, 'cookie': rev_cookie,
                    'src_dpid': path[-1], 'direction': 'rev', 'kind': 'tcp',
                    'bw_limit': rev_limit, 'timestamp': now,
                })
        else:
            installed = self.install_flow(path, eth.ethertype, src_ip, dst_ip, src_port, dst_port,
                                          in_port, msg.buffer_id, msg.data, setting.PRIORITY_TCP,
                                          ip_proto=(17 if udp_pkt is not None else 6))
            if not installed and dedup is not None:
                dedup.forget(dedup_key)

        # --- Step 8: If MPTCP, hand parsed payload to the serial MPTCP worker ---
        if is_mptcp:
            pool.dispatch_mptcp({
                'ev': ev,
                'pkt': pkt,
                'option': mptcp_option,
            })

    def _process_mptcp_worker(self, payload):
        """
        Process an MPTCP control packet on the dedicated serial MPTCP worker greenlet.
        Called by PacketInWorkerPool._mptcp_loop() — only ONE greenlet ever calls this,
        which makes it the single writer of mptcp_flows and of the subflow cookies.

        The regular worker builds the payload from the packet it already parsed
        and the kind-30 TCP option it captured (no re-parse here): {'ev', 'pkt', 'option'}.
        """
        self.packet_in_handler_MPTCP(payload['ev'].msg, payload['pkt'], payload['option'])

    # ------------------------------------------------------------------ #
    # MPTCP connection table: LRU-capped store + client-endpoint index    #
    # All writes happen on the single serial MPTCP worker greenlet.       #
    # ------------------------------------------------------------------ #
    def _host_id(self, ip):
        """Host identity for dual-homed hosts: a host owns 10.0.A.X and 10.0.B.X,
        so the last octet (X) identifies the machine across its interfaces. Lets a
        toward-initiator MP_JOIN aimed at the client's *other* address still
        correlate to the same connection."""
        try:
            return ip.rsplit('.', 1)[-1]
        except Exception:
            return ip

    def _client_ep_key(self, client_ip, client_port):
        """Reverse-index key: (client_host_id, client_port). The client's source
        port is unique per connection, so this stays unique even though dst ports
        are reused across different host pairs by design."""
        return (self._host_id(client_ip), client_port)

    def _mptcp_touch(self, token):
        """Mark a connection most-recently-used so LRU eviction spares it and refresh
        its subflows' timestamps so the MPTCP_FLOW_TTL idle timer restarts. Runs
        only on the serial MPTCP worker greenlet."""
        conn = self.mptcp_flows.get(token)
        if conn is not None:
            self.mptcp_flows.move_to_end(token)
            now = time.time()
            for sf in conn.values():
                sf['timestamp'] = now

    def _mptcp_put_subflow(self, token, subflow_id, entry, client_ep=None):
        """Add/update a subflow under `token`. Keeps the connection LRU-fresh,
        maintains the client-endpoint reverse index, and evicts the oldest
        connection(s) when over MPTCP_FLOWS_MAX. Runs only on the serial MPTCP
        worker greenlet, the single writer of these tables; the update has no
        yield point, so REST readers never see the table and its index out of step."""
        cap = setting.MPTCP_FLOWS_MAX
        conn = self.mptcp_flows.get(token)
        if conn is None:
            conn = {}
            self.mptcp_flows[token] = conn
        conn[subflow_id] = entry
        self.mptcp_flows.move_to_end(token)
        if client_ep is not None:
            self.mptcp_client_ep_index[client_ep] = token
            self.mptcp_token_ep[token] = client_ep
        # LRU eviction: drop oldest connections beyond the cap; purge their index.
        while len(self.mptcp_flows) > cap:
            old_token, _ = self.mptcp_flows.popitem(last=False)
            old_ep = self.mptcp_token_ep.pop(old_token, None)
            if old_ep is not None and self.mptcp_client_ep_index.get(old_ep) == old_token:
                self.mptcp_client_ep_index.pop(old_ep, None)

    def _cleanup_mptcp_state(self):
        """
        Remove expired MPTCP entries. Called periodically by the serial MPTCP worker
        greenlet, the single writer of these tables; the deletions have no yield
        point, so REST readers never observe a half-removed connection.
        """
        now = time.time()
        flow_ttl = setting.MPTCP_FLOW_TTL

        # Clean stale MPTCP connections (long TTL for tracking)
        expired_tokens = []
        for token, subflows in self.mptcp_flows.items():
            if all(now - sf.get('timestamp', now) > flow_ttl
                   for sf in subflows.values()):
                expired_tokens.append(token)
        for token in expired_tokens:
            self.mptcp_flows.pop(token, None)
            # keep the reverse index in lockstep with the table
            ep = self.mptcp_token_ep.pop(token, None)
            if ep is not None and self.mptcp_client_ep_index.get(ep) == token:
                self.mptcp_client_ep_index.pop(ep, None)

        if expired_tokens:
            LOG.info(f"[MPTCP Cleanup] Removed {len(expired_tokens)} expired connections")

    def packet_in_handler_MPTCP(self, msg, pkt, option):
        """
        Handle an MPTCP control packet. Runs only on the serial MPTCP worker
        greenlet, so it is the single writer of mptcp_flows / the reverse index.
        `option` is the kind-30 TCP option the packet-in worker captured from
        `pkt`; it is classified here (inspect_mptcp_packet) exactly once.

        MPTCP v1 (RFC 8684):
          * MP_CAPABLE SYN     -- 4 bytes, NO key: nothing to register, just skip
                                  (the generic worker already installed a TCP flow).
          * MP_CAPABLE SYN+ACK -- carries the server key -> connection token. This is
                                  where a connection is registered (idempotently).
          * MP_JOIN SYN        -- carries a connection token. Toward the server it
                                  resolves directly; toward the initiator the token is
                                  the initiator's (unlearnable from the SYN+ACK), so we
                                  correlate by the client endpoint via
                                  mptcp_client_ep_index instead of minting an orphan.
        """
        ipv4_pkt = pkt.get_protocol(ipv4.ipv4)
        receiver_token, subtype = self.inspect_mptcp_packet(pkt, ipv4_pkt, option)
        if ipv4_pkt is None or subtype is None:
            return
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        eth = pkt.get_protocol(ethernet.ethernet)
        if tcp_pkt is None or eth is None:
            return
        src_dpid, dst_dpid = self.get_switches(ipv4_pkt.src, ipv4_pkt.dst)
        if not src_dpid or not dst_dpid:
            return

        is_syn = (tcp_pkt.bits & 0x02) != 0
        is_ack = (tcp_pkt.bits & 0x10) != 0

        # ---------------------------------------------------------------- #
        # MP_CAPABLE                                                        #
        # ---------------------------------------------------------------- #
        if subtype == 0:
            if not (is_syn and is_ack):
                # v1 MP_CAPABLE SYN carries no key (and a bare/data MP_CAPABLE has
                # nothing to act on) -> skip; the generic TCP flow forwards it.
                return
            # SYN+ACK = connection accepted. Derive the server token (Key-B).
            receiver_token = self.derive_token_v1(option.value[2:10])
            if receiver_token in self.mptcp_flows:
                # Idempotent: a retransmitted SYN+ACK must not wipe existing subflows.
                self._mptcp_touch(receiver_token)
                return
            # Forward (client->server) path for subflow_1 (SYN+ACK is server->client).
            if self.use_agent1:
                top_paths = self._get_path_agent1_fast(dst_dpid, src_dpid)
                if top_paths:
                    # New connection (known tokens returned early above): uniform
                    # random pick from the top-N set so first subflows spread
                    # across the top paths instead of all landing on the top-1.
                    chosen = random.choice(top_paths)
                    path_key, path = chosen[0], chosen[1]
                else:
                    path_key, path = None, None
            else:
                path_key, path = self.get_path(dst_dpid, src_dpid)
            cookieid = self.get_cookie_ID()
            lim = self._bw_limit_for(path_key)
            installed = self.install_flow(path, eth.ethertype,
                                          ipv4_pkt.dst, ipv4_pkt.src,            # src=client, dst=server
                                          tcp_pkt.dst_port, tcp_pkt.src_port,    # client port, server port
                                          msg.match['in_port'], msg.buffer_id, msg.data,
                                          setting.PRIORITY_MPTCP, cookie=cookieid,
                                          limit=lim)
            subflowid = 'subflow_%d' % cookieid
            entry = {
                'src_ip': ipv4_pkt.dst, 'dst_ip': ipv4_pkt.src,
                'tcp_src_port': tcp_pkt.dst_port, 'tcp_dst_port': tcp_pkt.src_port,
                'eth_type': eth.ethertype, 'in_port': msg.match['in_port'],
                'buffer_id': msg.buffer_id, 'data': msg.data,
                'path_key': path_key, 'path': path,
                'cookie': cookieid, 'src_dpid': dst_dpid,
                'timestamp': time.time(),
            }
            # Index by the client endpoint so a later toward-initiator MP_JOIN (which
            # targets this client ip/port) can correlate to this connection.
            client_ep = self._client_ep_key(ipv4_pkt.dst, tcp_pkt.dst_port)
            self._mptcp_put_subflow(receiver_token, subflowid, entry, client_ep=client_ep)
            if installed:
                self._enqueue_track_cookie(cookieid, dst_dpid, path_key)
            return

        # ---------------------------------------------------------------- #
        # MP_JOIN                                                           #
        # ---------------------------------------------------------------- #
        if subtype == 1:
            target_token = None
            new_subflow_id = None

            if receiver_token in self.mptcp_flows:
                # Subflow toward the server of a known connection (resolves directly).
                target_token = receiver_token
                if self._subflow_exists(receiver_token, ipv4_pkt, tcp_pkt):
                    # retransmitted MP_JOIN SYN — don't add a duplicate subflow
                    self._mptcp_touch(receiver_token)
                    return
                cookieid = self.get_cookie_ID()
                new_subflow_id = 'subflow_%d' % cookieid
                if self.use_agent1 and self.agent1_ready:
                    path_key, new_path = self.get_alternative_path_using_agent1(receiver_token, src_dpid, dst_dpid)
                else:
                    path_key, new_path = self.get_alternative_path(receiver_token, src_dpid, dst_dpid)
                entry = self._mk_join_entry(ipv4_pkt, tcp_pkt, eth, msg, src_dpid,
                                            path_key, new_path, cookieid)
                self._mptcp_put_subflow(target_token, new_subflow_id, entry)
            else:
                # Token not directly known: correlate by the client endpoint this MP_JOIN
                # targets (a toward-initiator subflow carries the initiator's token).
                recovered = None
                if setting.ENABLE_MPTCP_JOIN_CORRELATION:
                    ep = self._client_ep_key(ipv4_pkt.dst, tcp_pkt.dst_port)
                    cand = self.mptcp_client_ep_index.get(ep)
                    if cand is not None and cand in self.mptcp_flows:
                        recovered = cand
                if recovered is not None and self._subflow_exists(recovered, ipv4_pkt, tcp_pkt):
                    # retransmitted toward-initiator MP_JOIN — already attached
                    self._mptcp_touch(recovered)
                    return
                cookieid = self.get_cookie_ID()
                new_subflow_id = 'subflow_%d' % cookieid
                if self.use_agent1:
                    if recovered is not None:
                        # Connection recovered via the client-endpoint index: its
                        # used paths are known, so apply the same no-shared-path
                        # random pick as a known-token MP_JOIN.
                        path_key, path = self.get_alternative_path_using_agent1(
                            recovered, src_dpid, dst_dpid)
                    else:
                        # Orphan first subflow: uniform random pick from the top-N set.
                        top_paths = self._get_path_agent1_fast(src_dpid, dst_dpid)
                        if top_paths:
                            chosen = random.choice(top_paths)
                            path_key, path = chosen[0], chosen[1]
                        else:
                            path_key, path = None, None
                else:
                    path_key, path = self.get_path(src_dpid, dst_dpid)
                if recovered is not None:
                    # Attach the toward-initiator subflow to the existing connection.
                    target_token = recovered
                    entry = self._mk_join_entry(ipv4_pkt, tcp_pkt, eth, msg, src_dpid,
                                                path_key, path, cookieid)
                    self._mptcp_put_subflow(target_token, new_subflow_id, entry)
                else:
                    # Genuinely unknown -> register under its own token (LRU-bounded),
                    # indexed by client endpoint so later joins/SYN+ACK can correlate.
                    LOG.warning("[Forwarding] WARN: MP_JOIN received for unknown MPTCP connection")
                    target_token = receiver_token
                    entry = self._mk_join_entry(ipv4_pkt, tcp_pkt, eth, msg, src_dpid,
                                                path_key, path, cookieid)
                    ep = self._client_ep_key(ipv4_pkt.dst, tcp_pkt.dst_port)
                    self._mptcp_put_subflow(target_token, new_subflow_id, entry, client_ep=ep)

            if target_token is not None and new_subflow_id is not None:
                sf = self.mptcp_flows[target_token][new_subflow_id]
                lim = self._bw_limit_for(sf['path_key'])
                installed = self.install_flow(sf['path'], sf['eth_type'], sf['src_ip'], sf['dst_ip'],
                                              sf['tcp_src_port'], sf['tcp_dst_port'], sf['in_port'],
                                              sf['buffer_id'], sf['data'], setting.PRIORITY_MPTCP,
                                              cookie=sf['cookie'], limit=lim)
                if installed:
                    self._enqueue_track_cookie(sf['cookie'], sf['src_dpid'], sf['path_key'])
            return

        LOG.warning("[Forwarding][packet_in_handler_MPTCP] Invalid MPTCP packet received")

    def _subflow_exists(self, token, ipv4_pkt, tcp_pkt):
        """True if a subflow with this exact 4-tuple is already tracked under `token`
        (e.g. a retransmitted MP_JOIN SYN) — avoids duplicate subflow entries."""
        conn = self.mptcp_flows.get(token)
        if not conn:
            return False
        for sf in conn.values():
            if (sf.get('src_ip') == ipv4_pkt.src and sf.get('dst_ip') == ipv4_pkt.dst and
                    sf.get('tcp_src_port') == tcp_pkt.src_port and
                    sf.get('tcp_dst_port') == tcp_pkt.dst_port):
                return True
        return False

    def _mk_join_entry(self, ipv4_pkt, tcp_pkt, eth, msg, src_dpid, path_key, path, cookieid):
        """Build a subflow record for an MP_JOIN (direction as seen on the wire)."""
        return {
            'src_ip': ipv4_pkt.src, 'dst_ip': ipv4_pkt.dst,
            'tcp_src_port': tcp_pkt.src_port, 'tcp_dst_port': tcp_pkt.dst_port,
            'eth_type': eth.ethertype, 'in_port': msg.match['in_port'],
            'buffer_id': msg.buffer_id, 'data': msg.data,
            'src_dpid': src_dpid, 'path_key': path_key, 'path': path,
            'cookie': cookieid, 'timestamp': time.time(),
        }

    def get_cookie_ID(self):
        """
        Returns the next MPTCP subflow cookie: MPTCP_COOKIE_BASE | counter
        (bits 31..29 = 010). Plain-TCP dynamic flows use get_tcp_cookie_ID()
        (TCP_COOKIE_BASE | counter, bits 31..29 = 011). Both spaces match
        setting.DYNAMIC_COOKIE_MASK, so the FlowRemoved handler recognises every
        dynamic flow with one mask; setting.cookie_kind() tells them apart. The
        cookie is the handle that ties the switch's FlowRemoved back to the
        meter to release and the flow-ledger entry to drop. Punts, UDP flows and
        the MPTCP pre-install carry cookie 0.
        The counter wraps within [1, setting.MAX_COOKIE_ID] (29 bits). No lock:
        under eventlet this read-modify-write has no yield point.
        """
        self.mptcp_flows_cookie += 0x1
        if self.mptcp_flows_cookie > self.mptcp_flows_cookie_max:
            LOG.warning("Max MPTCP cookie ID reached. Resetting to default.")
            self.mptcp_flows_cookie = setting.COOKIE_ID + 0x1
        return setting.MPTCP_COOKIE_BASE | self.mptcp_flows_cookie

    def get_tcp_cookie_ID(self):
        """Next plain-TCP dynamic-flow cookie: TCP_COOKIE_BASE | counter (see get_cookie_ID)."""
        self.tcp_flows_cookie += 0x1
        if self.tcp_flows_cookie > self.mptcp_flows_cookie_max:
            LOG.warning("Max TCP cookie ID reached. Resetting to default.")
            self.tcp_flows_cookie = setting.COOKIE_ID + 0x1
        return setting.TCP_COOKIE_BASE | self.tcp_flows_cookie

    # ------------------------------------------------------------------ #
    # Energy-aware BW limiting: per-path limit lookup + reverse path key   #
    # ------------------------------------------------------------------ #
    def _bw_limit_for(self, path_key):
        """
        Per-flow cap (Mbps) for a path, from the table computed once per Agent1
        cycle (_refresh_bw_limits). O(1) dict lookup on the packet-in hot path.
        Returns None when BW limiting is disabled. In BW_LIMIT_DRY_RUN the limit is
        still returned (and recorded in the ledger) but MeterManager installs no meter.
        """
        if self.meter_manager is None or path_key is None:
            return None
        return self.path_bw_limit.get(path_key, setting.BW_DEFAULT_Mbps)

    _REVERSE_MISS = object()

    def _reverse_path_key(self, path_key, dpids):
        """
        Path key of the reverse path (same switches, opposite direction), e.g.
        '201-202-path1' -> '202-201-path8'. Looked up among the <=k 'dst-src-pathN'
        keys and memoised (the cache is a pure function of path_db; it is cleared
        when path_db changes size). Returns None when no reverse path exists.
        """
        if path_key is None or not dpids:
            return None
        if len(self.path_db) != self._reverse_cache_path_db_len:
            self._reverse_path_key_cache = {}
            self._reverse_cache_path_db_len = len(self.path_db)
        cached = self._reverse_path_key_cache.get(path_key, self._REVERSE_MISS)
        if cached is not self._REVERSE_MISS:
            return cached
        rev = [d for d in reversed(list(dpids))]
        prefix = f"{rev[0]}-{rev[-1]}-path"
        result = None
        n = 1
        while n <= 4 * max(1, int(setting.k)):
            cand = self.path_db.get(f"{prefix}{n}")
            if cand is None:
                break
            if [node[0] for node in cand] == rev:
                result = f"{prefix}{n}"
                break
            n += 1
        if result is None:
            self._warn_once(f"revkey:{prefix}", f"[BW] no reverse path key found for {path_key}; using the forward key")
        self._reverse_path_key_cache[path_key] = result
        return result

    def _warn_once(self, key, msg):
        if key not in self._warned_once:
            self._warned_once.add(key)
            LOG.warning(msg)

    # ------------------------------------------------------------------ #
    # Flow ledger (single writer: worker pool agent-report greenlet)        #
    # ------------------------------------------------------------------ #
    def ledger_add(self, cookie, path_key, direction='fwd', ingress_dpid=None, kind='tcp', bw_limit=None, ts=None):
        """Record a cookied flow entry (called for every flow_installed / mptcp_track_cookie report)."""
        if cookie is None or path_key is None:
            return
        ts = ts or time.time()
        old = self.flow_ledger.get(cookie)
        if old is not None:
            self._ledger_dec(old)
        self.flow_ledger[cookie] = {'path_key': path_key, 'direction': direction,
                                    'ingress_dpid': ingress_dpid, 'kind': kind,
                                    'bw_limit': bw_limit, 'ts': ts}
        counter = self.path_active_flows if direction == 'fwd' else self.path_active_flows_rev
        counter[path_key] = counter.get(path_key, 0) + 1
        if direction == 'fwd':
            self.path_activity_ts_fwd[path_key] = ts

    def _ledger_dec(self, entry):
        counter = self.path_active_flows if entry.get('direction') == 'fwd' else self.path_active_flows_rev
        pk = entry.get('path_key')
        n = counter.get(pk, 0) - 1
        if n <= 0:
            counter.pop(pk, None)
        else:
            counter[pk] = n

    def ledger_remove(self, cookie, ts=None):
        """Drop a ledger entry (FlowRemoved or switch departure). Returns the entry or None."""
        entry = self.flow_ledger.pop(cookie, None)
        if entry is not None:
            self._ledger_dec(entry)
            ts = ts or time.time()
            pk = entry.get('path_key')
            if entry.get('direction') == 'fwd':
                self.path_activity_ts_fwd[pk] = ts
        return entry

    def get_active_flow_count_by_path(self, include_reverse=None):
        """{path_key: live flows} from the ledger (forward only unless include_reverse)."""
        if include_reverse is None:
            include_reverse = setting.BW_N_ACTIVE_INCLUDE_REVERSE
        counts = dict(self.path_active_flows)
        if include_reverse:
            for pk, n in list(self.path_active_flows_rev.items()):
                counts[pk] = counts.get(pk, 0) + n
        return counts

    def get_paths_with_activity(self, window_s):
        """Set of path keys whose forward direction saw an install or removal within
        the last window_s seconds."""
        now = time.time()
        return {pk for pk, ts in list(self.path_activity_ts_fwd.items()) if now - ts <= window_s}

    def ledger_drop_dpid(self, dpid):
        """A switch left: drop every ledger entry whose ingress is that dpid (its flow
        entries, and their FlowRemoved, are gone with it). One O(ledger) pass, no yield
        points; called on the agent-report greenlet (or inline when the report queue is full)."""
        gone = [c for c, e in self.flow_ledger.items() if e.get('ingress_dpid') == dpid]
        for c in gone:
            self.ledger_remove(c)
        if gone:
            self.lifecycle_stats['ledger_dropped_dead_dpid'] = \
                self.lifecycle_stats.get('ledger_dropped_dead_dpid', 0) + len(gone)
            LOG.info(f"[BW] dpid {dpid} left: dropped {len(gone)} ledger entries")

    def get_ledger_counts(self):
        """Counts of live ledger entries by kind and direction (REST)."""
        out = {'total': 0, 'tcp_fwd': 0, 'tcp_rev': 0, 'mptcp': 0, 'other': 0,
               'paths_with_fwd_flows': len(self.path_active_flows)}
        for e in list(self.flow_ledger.values()):
            out['total'] += 1
            k = e.get('kind')
            if k == 'tcp':
                out['tcp_fwd' if e.get('direction') == 'fwd' else 'tcp_rev'] += 1
            elif k == 'mptcp':
                out['mptcp'] += 1
            else:
                out['other'] += 1
        return out

    # ------------------------------------------------------------------ #
    # MPTCP option parsing and subflow path selection                      #
    # ------------------------------------------------------------------ #
    def inspect_mptcp_packet(self, pkt, ipv4_pkt, option):
        """
        Classify one MPTCP option (kind 30). Returns (receiver_token, subtype):
        (None, 0) for MP_CAPABLE, (token, 1) for MP_JOIN, (None, None) for anything
        else or an option length other than the 12/20-byte control forms.
        """
        if not ipv4_pkt:
            return None, None

        tcp_pkt = pkt.get_protocol(tcp.tcp)
        if not tcp_pkt:
            return None, None

        if not tcp_pkt.option:
            return None, None

        if option.length not in [12, 20]:
            return None, None
        LOG.debug("[Forwarding][inspect_mptcp_packet] Start of MPTCP packet inspection")
        mptcp_option_bytes = option.value
        subtype_flags = mptcp_option_bytes[0]
        subtype = (subtype_flags >> 4) & 0x0F
        if subtype == 0:  # MP_CAPABLE
            return None, subtype
        elif subtype == 1:  # MP_JOIN
            receiver_token_bytes = mptcp_option_bytes[2:6]
            receiver_token = int.from_bytes(receiver_token_bytes, 'big')
            return receiver_token, subtype

        return None, None

    def derive_token_v1(self, sender_key_bytes: bytes) -> int:
        """
        Derives a 32-bit MPTCP v1 token from the sender's 64-bit key.

        MPTCP v1 (RFC 8684 / Linux mainline):
            token = trunc_32( SHA-256(KEY) )
        """
        hash_bytes = hashlib.sha256(sender_key_bytes).digest()
        token_bytes = hash_bytes[:4]          # first 4 bytes
        token = int.from_bytes(token_bytes, 'big')
        return token

    def _pick_subflow_path(self, all_paths, used_paths):
        """
        Uniform random pick among candidate paths not already carrying a
        subflow of this MPTCP connection. A path and its reverse count as the
        same path (a subflow recorded in the opposite direction rides the same
        links). When every candidate is taken, pick uniformly among the
        least-used ones. Pure CPU, no yield points — safe on the serial MPTCP
        worker greenlet.

        Args:
            all_paths: non-empty list of (path_key, path), path = list of dpids.
            used_paths: paths of the connection's existing subflows (entries
                may be None when an earlier selection failed — skipped).
        Returns:
            (path_key, path)
        """
        used = set()
        for up in used_paths:
            if up:
                used.add(tuple(up))
                used.add(tuple(reversed(up)))

        unused = [(k, p) for k, p in all_paths if tuple(p) not in used]
        if unused:
            path_key, path = random.choice(unused)
            if self.agent1_logging:
                LOG.info(f"[Forwarding][MPTCP path pick] {len(unused)}/{len(all_paths)} "
                         f"paths unused, selected {path_key}")
            return path_key, list(path)

        # Every candidate already carries a subflow of this connection:
        # spread uniformly over the least-used ones.
        def _canon(p):
            return min(tuple(p), tuple(reversed(p)))
        usage = {}
        for up in used_paths:
            if up:
                c = _canon(up)
                usage[c] = usage.get(c, 0) + 1
        min_usage = min(usage.get(_canon(p), 0) for _, p in all_paths)
        least_used = [(k, p) for k, p in all_paths if usage.get(_canon(p), 0) == min_usage]
        path_key, path = random.choice(least_used)
        if self.agent1_logging:
            LOG.info(f"[Forwarding][MPTCP path pick] all {len(all_paths)} paths used, "
                     f"selected least-used {path_key} (usage={min_usage})")
        return path_key, list(path)

    def get_alternative_path(self, sender_token, src_dpid, dst_dpid):
        """
        Picks a path for a new MPTCP subflow without Agent1 probabilities:
        candidates are the first TOP_N_PATHS entries of path_db (Yen order,
        shortest first); the pick is uniform random among paths this
        connection's subflows do not already use (_pick_subflow_path).
        """
        temp_path_db = self.path_db
        temp_mptcp_flows = self.mptcp_flows

        prefix = f"{src_dpid}-{dst_dpid}-path"
        candidates = []
        for path_key, path in temp_path_db.items():
            if path_key.startswith(prefix):
                simple_path = [node[0] for node in path]
                candidates.append((path_key, simple_path))

        if not candidates:
            return None, None

        # Keep the TOP_N_PATHS shortest (path1..pathN are Yen-ordered; sort on
        # the numeric suffix so path2 comes before path10).
        def _path_index(item):
            try:
                return int(item[0].rsplit('path', 1)[-1])
            except ValueError:
                return 1 << 30
        candidates.sort(key=_path_index)
        top_n = setting.TOP_N_PATHS
        candidates = candidates[:top_n]

        used_paths = []
        if sender_token in temp_mptcp_flows:
            used_paths = [subflow['path'] for subflow in temp_mptcp_flows[sender_token].values()]

        return self._pick_subflow_path(candidates, used_paths)

    def get_alternative_path_using_agent1(self, receiver_token, src_dpid, dst_dpid):
        """
        Picks a path for a new MPTCP subflow from the top-N paths ranked by
        Agent1's probabilities — the same set the TCP workers draw from. The
        pick is uniform random among paths this connection's subflows do not
        already use (_pick_subflow_path).
        """
        path_db_prob = self.Path_db_probability
        temp_mptcp_flows = self.mptcp_flows

        if not path_db_prob:
            LOG.warning(f"[Forwarding][Agent 1] WARN: Path database is empty for src_dpid: {src_dpid}, dst_dpid: {dst_dpid}")
            return self.get_alternative_path(receiver_token, src_dpid, dst_dpid)

        top = self.pair_top_paths.get((src_dpid, dst_dpid)) if self.pair_top_paths else None
        if top:
            all_paths = [(k, p) for k, p, _ in top]
        else:
            prefix = f"{src_dpid}-{dst_dpid}-path"
            candidates = []
            for path_key, path_data in path_db_prob.items():
                if path_key.startswith(prefix):
                    candidates.append((path_key, path_data['path'],
                                       path_data.get('probability', 0.0)))
            if not candidates:
                return None, None
            # Same top-N set the TCP workers draw from: probabilities rank the
            # set members; the pick within the set is uniform.
            candidates.sort(key=lambda x: x[2], reverse=True)
            top_n = setting.TOP_N_PATHS
            all_paths = [(k, p) for k, p, _ in candidates[:top_n]]

        used_paths = []
        if receiver_token in temp_mptcp_flows:
            used_paths = [subflow['path'] for subflow in temp_mptcp_flows[receiver_token].values()]
        if self.agent1_logging:
            LOG.info(f"[Forwarding][Agent 1] used_paths: {used_paths}")

        return self._pick_subflow_path(all_paths, used_paths)

    def get_switches(self, src_ip, dst_ip):
        """
        Determines the source and destination switches for given source and destination IPs.
        Returns (src_dpid, dst_dpid) or (None, None) if not found.
        """
        src_dpid = dst_dpid = None

        src_result = self.discovery.get_host_ip_location(src_ip)
        if src_result is not None:
            src_dpid = src_result[0]

        dst_result = self.discovery.get_host_ip_location(dst_ip)
        if dst_result is not None:
            dst_dpid = dst_result[0]

        return src_dpid, dst_dpid

    def get_path_random(self, src_dpid, dst_dpid):
        """
        Uniform random pick among the pair's paths in path_db (used before Agent1 has
        produced probabilities). Returns (path_key, [dpid, ...]) or (None, None).
        """
        paths = []
        for path_key in self.path_db.keys():
            if path_key.startswith(f"{src_dpid}-{dst_dpid}-path"):
                paths.append(path_key)

        if not paths:
            return None, None

        path_key = random.choice(paths)
        path_detailed = self.path_db.get(path_key, None)
        path = []
        for i in path_detailed:
            path.append(i[0])
        return path_key, path

    def get_path(self, src_dpid, dst_dpid):
        """
        Shortest path of the pair (path1 in path_db). Returns (path_key, [dpid, ...]) or (None, None).
        """
        path_key = f"{src_dpid}-{dst_dpid}-path1"
        path_detailed = self.path_db.get(path_key, None)
        if path_detailed is None:
            LOG.warning(f"[Forwarding][Getting path] WARN: Path not found for {path_key}. ")
            return None, None
        path = []
        for i in path_detailed:
            path.append(i[0])
        return path_key, path

    def _build_pair_top_paths(self, sorted_probs, path_db_prob):
        """{(src_dpid, dst_dpid): [(path_key, dpid_list, prob), ...]} - the first TOP_N_PATHS
        keys of each pair in probability order (sorted_probs is sorted descending)."""
        top_n = setting.TOP_N_PATHS
        pair_top = {}
        for pathid, prob in sorted_probs.items():
            parts = pathid.split('-', 2)
            if len(parts) < 3:
                continue
            try:
                pair = (int(parts[0]), int(parts[1]))
            except ValueError:
                continue
            lst = pair_top.get(pair)
            if lst is None:
                lst = pair_top[pair] = []
            if len(lst) < top_n:
                entry = path_db_prob.get(pathid)
                if entry is not None:
                    lst.append((pathid, entry['path'], prob))
        return pair_top

    def _egress_out_port_rev(self, path, src_ip):
        """Out port at path[-1] for the REVERSE direction (toward path[-2], or the client's
        host port on a single-switch path); None when unknown."""
        if path and len(path) > 1:
            pp = self.discovery.link_port_table.get((path[-1], path[-2]))
            return pp[0] if pp else None
        return self.get_port(src_ip)

    def _ingress_out_port(self, path, dst_ip):
        """Out port of the ingress switch for `path` (O(1)); None when unknown."""
        if path and len(path) > 1:
            pp = self.discovery.link_port_table.get((path[0], path[1]))
            return pp[0] if pp else None
        return self.get_port(dst_ip)

    def _get_path_agent1_fast(self, src_dpid, dst_dpid):
        """
        Fast path selection using pre-computed probabilities.
        Returns top N paths as list of (path_key, path, probability).
        NO file I/O. Used by worker threads on the hot path.
        Falls back to get_path_random if probabilities not yet computed.

        The PathCache stores this list and picks uniform random on each get(),
        distributing traffic evenly across the top-N paths (probabilities only
        decide set membership).
        """
        top = self.pair_top_paths.get((src_dpid, dst_dpid)) if self.pair_top_paths else None
        if top:
            return top                    # O(1): built once per Agent1 cycle

        path_db_prob = self.Path_db_probability
        if not path_db_prob:
            result = self.get_path_random(src_dpid, dst_dpid)
            if result[0] is None:
                return None
            return [(result[0], result[1], 1.0)]

        prefix = f"{src_dpid}-{dst_dpid}-path"
        candidates = []

        for path_key, path_data in path_db_prob.items():
            if path_key.startswith(prefix):
                prob = path_data.get('probability', 0.0)
                candidates.append((path_key, path_data['path'], prob))

        if not candidates:
            result = self.get_path_random(src_dpid, dst_dpid)
            if result[0] is None:
                return None
            return [(result[0], result[1], 1.0)]

        # Sort by probability descending, take top N
        candidates.sort(key=lambda x: x[2], reverse=True)
        top_n = setting.TOP_N_PATHS
        return candidates[:top_n]

    def install_flow(self, path, eth_type, src_ip, dst_ip, src_ip_port, dst_ip_port, in_port, buffer_id, data,
                     priority, cookie=None, bidirectional=False,
                     limit=None, reverse_cookie=None, reverse_limit=None, ip_proto=6):
        """
        Installs flow entries along the path to set up traffic flows from source to destination.

        bidirectional=True (plain TCP) also installs the reverse dst->src entries on the same
        path; MPTCP callers leave it False so the reply direction still packet-ins.
        Cookies: `cookie` is stamped on every forward hop, `reverse_cookie` on every reverse hop
        (a cookie is required for the FlowRemoved -> meter/ledger bookkeeping).
        Meters (energy-aware BW limiting): ONLY the INGRESS FlowMod of each direction carries
        the meter - forward at path[0] with `limit`, reverse at path[-1] with `reverse_limit` -
        and only those ingress FlowMods (when cookied) request OFPFF_SEND_FLOW_REM. Intermediate
        hops carry the cookie but no meter. `ip_proto` (6/17) selects the L4 match fields.
        Returns True when the FlowMods were sent, False on any early exit (callers must not
        record ledger entries for flows that were never installed).
        """
        if path is None or len(path) == 0:
            LOG.error("install_flow: Path Error")
            return False
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug(f"[Forwarding][install_flow] path: {path} {src_ip}->{dst_ip} pri={priority} "
                      f"cookie={cookie} limit={limit} bidir={bidirectional} rev_cookie={reverse_cookie}")

        first_dp = self.monitor.datapaths_table.get(path[0])
        if first_dp is None:
            LOG.debug(f"[install_flow] Datapath {path[0]} not found in datapaths_table, skipping")
            return False

        def fwd(dp, in_p, out_p, is_ingress):
            self.send_flow_mod(dp, eth_type, src_ip, dst_ip, src_ip_port, dst_ip_port, in_p, out_p, priority,
                               cookie=cookie,
                               BW_limit=(limit if is_ingress else None),
                               send_flow_rem=bool(is_ingress and cookie is not None),
                               ip_proto=ip_proto)

        def rev(dp, in_p, out_p, is_ingress):
            if not bidirectional:
                return
            self.send_flow_mod(dp, eth_type, dst_ip, src_ip, dst_ip_port, src_ip_port, in_p, out_p, priority,
                               cookie=reverse_cookie,
                               BW_limit=(reverse_limit if is_ingress else None),
                               send_flow_rem=bool(is_ingress and reverse_cookie is not None),
                               ip_proto=ip_proto)

        if len(path) > 2:
            for i in range(1, len(path) - 1):
                port_pair = self.get_port_pair(path[i - 1], path[i])
                port_pair_next = self.get_port_pair(path[i], path[i + 1])
                if port_pair and port_pair_next:
                    src_port, dst_port = port_pair[1], port_pair_next[0]
                    datapath = self.monitor.datapaths_table.get(path[i])
                    if datapath is None:
                        LOG.error(f"Datapath for node {path[i]} not found.")
                        continue
                    fwd(datapath, src_port, dst_port, False)
                    rev(datapath, dst_port, src_port, False)
                else:
                    LOG.error(f"install_flow: Path Error at node {path[i]}")
                    return False

        if len(path) > 1:
            port_pair = self.get_port_pair(path[-2], path[-1])
            if port_pair is None:
                LOG.error("[forwarding][install_flow][port_pair is None]: Port not found for last hop")
                return False
            src_port = port_pair[1]
            dst_port = self.get_port(dst_ip)
            if dst_port is None:
                LOG.error("[forwarding][install_flow][dst_port is None]: Out port not found for destination IP")
                return False
            last_dp = self.monitor.datapaths_table.get(path[-1])
            if last_dp is None:
                LOG.debug(f"[install_flow] Last datapath {path[-1]} not found in datapaths_table, skipping")
                return False
            fwd(last_dp, src_port, dst_port, False)
            rev(last_dp, dst_port, src_port, True)      # reverse direction enters here

            port_pair = self.get_port_pair(path[0], path[1])
            if port_pair is None:
                LOG.error("install_flow: Port not found in first hop")
                return False
            out_port = port_pair[0]
            fwd(first_dp, in_port, out_port, True)      # forward ingress: meter + SEND_FLOW_REM
            rev(first_dp, out_port, in_port, False)
            if priority != setting.PRIORITY_MPTCP:
                self.send_packet_out(first_dp, buffer_id, in_port, out_port, data)
            return True
        else:
            out_port = self.get_port(dst_ip)
            if out_port is None:
                LOG.error("install_flow: Out port not found in same datapath")
                return False
            fwd(first_dp, in_port, out_port, True)
            rev(first_dp, out_port, in_port, True)
            if priority != setting.PRIORITY_MPTCP:
                # Send packet out only if not MPTCP flow
                self.send_packet_out(first_dp, buffer_id, in_port, out_port, data)
            return True

    def send_packet_out(self, datapath, buffer_id, in_port, out_port, data):
        """
        Sends the packet out after flow installation.
        """
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto

        actions = [parser.OFPActionOutput(out_port)]
        if buffer_id != ofproto.OFP_NO_BUFFER:
            data = None                      # the switch holds the packet; do not resend it
        out = parser.OFPPacketOut(
            datapath=datapath, buffer_id=buffer_id,
            in_port=in_port, actions=actions, data=data
        )
        datapath.send_msg(out)

    def get_port_pair(self, src_dpid, dst_dpid):
        # Get link ports
        if (src_dpid, dst_dpid) in self.discovery.link_port_table:
            return self.discovery.link_port_table[(src_dpid, dst_dpid)]
        else:
            LOG.info("get_port_pair: no link %s -> %s", src_dpid, dst_dpid)
            return None

    def get_port(self, dst_ip):
        """Access port of the host that owns dst_ip, from the topology manager's
        {ip: (dpid, port)} table (O(1)); None for an unknown host."""
        loc = self.discovery.hosts.get(dst_ip)
        return loc[1] if loc else None

    def send_flow_mod(self, datapath, eth_type, src_ip, dst_ip, src_ip_port, dst_ip_port, src_port, dst_port, priority,
                      cookie=None, BW_limit=None, send_flow_rem=False, ip_proto=6):
        """
        Build the match for one hop and hand it to add_flow. Match construction is
        independent of BW_limit; ip_proto (6 TCP / 17 UDP) selects the L4 fields.
        """
        parser = datapath.ofproto_parser

        actions = [parser.OFPActionOutput(dst_port)]
        final_hard_timeout = setting.HARD_TIMEOUT
        if eth_type == 0x0800:  # IPv4
            if src_ip_port and dst_ip_port:
                if ip_proto == 17:  # UDP
                    match = parser.OFPMatch(
                        in_port=src_port,
                        eth_type=eth_type,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        ip_proto=17,
                        udp_src=src_ip_port,
                        udp_dst=dst_ip_port
                    )
                else:  # TCP
                    match = parser.OFPMatch(
                        in_port=src_port,
                        eth_type=eth_type,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                        ip_proto=6,
                        tcp_src=src_ip_port,
                        tcp_dst=dst_ip_port
                    )
            else:  # General IPv4
                match = parser.OFPMatch(
                    in_port=src_port,
                    eth_type=eth_type,
                    ipv4_src=src_ip,
                    ipv4_dst=dst_ip
                )
        else:  # Non-IP packet
            match = parser.OFPMatch(
                in_port=src_port,
                eth_type=eth_type
            )
        self.add_flow(datapath, priority, match, actions, idle_timeout=setting.IDL_TIME,
                      hard_timeout=final_hard_timeout, cookie=cookie,
                      BW_limit=BW_limit, send_flow_rem=send_flow_rem)

    def add_flow(self, datapath, priority, match, actions, idle_timeout=0, hard_timeout=0, cookie=None,
                 BW_limit=None, send_flow_rem=False):
        """
        Install one flow entry. With BW_limit (Mbps) AND a cookie, MeterManager allocates a
        per-flow meter on this datapath and the FlowMod carries OFPInstructionMeter first.
        send_flow_rem requests OFPFF_SEND_FLOW_REM (only honoured when a cookie is present:
        the FlowRemoved handler needs it). Without BW_limit the FlowMod carries only the
        apply-actions instruction; a given cookie is still set (cookie_mask 0).
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = []
        metered = False
        mm = self.meter_manager
        if BW_limit is not None and mm is not None:
            if cookie is None:
                self._warn_once('add_flow_bw_no_cookie',
                                "[BW] add_flow called with BW_limit but no cookie; installing unmetered")
            else:
                meter_inst = mm.allocate(datapath, cookie, BW_limit)
                if meter_inst:
                    inst.extend(meter_inst)
                    metered = True
            self.lifecycle_stats['installs_metered' if metered else 'installs_unmetered'] += 1
        inst.append(parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions))

        kwargs = dict(datapath=datapath, priority=priority, match=match, instructions=inst,
                      idle_timeout=idle_timeout, hard_timeout=hard_timeout)
        if cookie is not None:
            kwargs['cookie'] = cookie
            kwargs['cookie_mask'] = 0
            if send_flow_rem:
                kwargs['flags'] = ofproto.OFPFF_SEND_FLOW_REM
        mod = parser.OFPFlowMod(**kwargs)
        datapath.send_msg(mod)

    # ------------------------------------------------------------------ #
    # Meter / flow lifecycle handlers (energy-aware BW limiting)           #
    # All @set_ev_cls handlers run on Forwarding's RyuApp event-loop        #
    # greenlet and are O(1) or enqueue-only; the work happens on the        #
    # worker pool's agent-report greenlet (single ledger writer).           #
    # ------------------------------------------------------------------ #
    @set_ev_cls(ofp_event.EventOFPFlowRemoved, MAIN_DISPATCHER)
    def _flow_removed_handler(self, ev):
        msg = ev.msg
        cookie = msg.cookie
        self.lifecycle_stats['flow_removed_events'] += 1
        if not setting.cookie_is_dynamic(cookie):
            self.lifecycle_stats['flow_removed_ignored'] += 1
            return
        report = {
            'type': 'flow_removed',
            'dpid': msg.datapath.id,
            'cookie': cookie,
            'byte_count': msg.byte_count,
            'packet_count': msg.packet_count,
            'duration_sec': msg.duration_sec,
            'reason': msg.reason,
            'priority': msg.priority,
            'idle_timeout': msg.idle_timeout,
            'ts': time.time(),
        }
        self.worker_pool.report_to_agent(report)

    def process_flow_removed(self, report):
        """
        Consume one flow_removed report from the switch. Runs on the agent-report greenlet:
        returns the meter to the idle pool (bookkeeping only - NO OpenFlow message, so this
        greenlet never blocks on a slow switch), drops the ledger entry, records activity
        and appends the per-flow JSONL record.
        """
        dpid = report.get('dpid')
        cookie = report.get('cookie')
        reason = report.get('reason')
        ts = report.get('ts') or time.time()
        meter_id = None
        if self.meter_manager is not None and dpid is not None:
            meter_id = self.meter_manager.release(dpid, cookie)   # pool bookkeeping only
        entry = self.ledger_remove(cookie, ts)
        path_key = entry.get('path_key') if entry else None
        self.lifecycle_stats['flow_removed_processed'] += 1
        by_reason = self.lifecycle_stats['flow_removed_by_reason']
        by_reason[str(reason)] = by_reason.get(str(reason), 0) + 1
        if setting.FLOW_REMOVED_LOG_ENABLED:
            self._energy_enqueue('flow_removed', {
                'ts': round(ts, 3), 'dpid': dpid, 'cookie': cookie,
                'kind': setting.cookie_kind(cookie),
                'direction': entry.get('direction') if entry else None,
                'path_key': path_key, 'meter_id': meter_id,
                'byte_count': report.get('byte_count'), 'packet_count': report.get('packet_count'),
                'duration_sec': report.get('duration_sec'), 'reason': reason,
                'priority': report.get('priority'),
                'bw_limit_mbps': entry.get('bw_limit') if entry else None,
                'installed_ts': round(entry['ts'], 3) if entry else None,
            })

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        """
        (Re)join: the switch-side meter table is unknown (a restarted switch lost its
        meters, a reconnected one kept them) -> reset bookkeeping and, when
        CLEAN_METERS_ON_JOIN, delete OFPM_ALL (this also deletes flow entries that
        reference meters - they re-punt). Then ask for the meter features so a datapath
        without meter support degrades to unmetered installs.
        """
        dp = ev.msg.datapath
        if self.meter_manager is None:
            return
        self.meter_manager.reset(dp.id, datapath=dp)
        try:
            dp.send_msg(dp.ofproto_parser.OFPMeterFeaturesStatsRequest(dp, 0))
        except Exception as e:
            LOG.warning(f"[BW] could not request meter features from dpid {dp.id}: {e}")

    @set_ev_cls(ofp_event.EventOFPMeterFeaturesStatsReply, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def _meter_features_reply_handler(self, ev):
        if self.meter_manager is None:
            return
        dp = ev.msg.datapath
        self.lifecycle_stats['meter_features_replies'] += 1
        for feat in ev.msg.body:
            max_meter = int(getattr(feat, 'max_meter', 0) or 0)
            band_types = getattr(feat, 'band_types', 0)
            self.meter_manager.set_capacity(dp.id, min(setting.MAX_METER_ID, max_meter))
            if max_meter <= 0:
                LOG.warning(f"[BW] dpid {dp.id} reports NO meter support (max_meter=0): installing unmetered there")
            else:
                LOG.info(f"[BW] dpid {dp.id} meter features: max_meter={max_meter} band_types={band_types:#x} "
                         f"capabilities={getattr(feat, 'capabilities', 0):#x} max_bands={getattr(feat, 'max_bands', '?')}")

    @set_ev_cls(ofp_event.EventOFPStateChange, [DEAD_DISPATCHER])
    def _dp_state_change_handler(self, ev):
        """Switch left: forget its meters (nothing to send) and drop its ledger entries
        through the single ledger writer (agent greenlet); inline when the report queue
        is full (the pass has no yield points)."""
        dp = ev.datapath
        if dp is None:
            return
        if self.meter_manager is not None:
            self.meter_manager.forget(dp.id)
        rep = {'type': 'dpid_gone', 'dpid': dp.id, 'ts': time.time()}
        if not self.worker_pool.report_to_agent(rep):
            self.ledger_drop_dpid(dp.id)

    @set_ev_cls(ofp_event.EventOFPErrorMsg, MAIN_DISPATCHER)
    def _error_msg_handler(self, ev):
        """Count every OpenFlow error; rate-limited log; meter-related errors back off that dpid."""
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        key = (msg.type, msg.code)
        self.of_error_counts[key] = self.of_error_counts.get(key, 0) + 1
        # A 1.3 switch rejects an unsupported OFPInstructionMeter with OFPBIC_UNSUP_INST.
        meter_related = (msg.type == ofp.OFPET_METER_MOD_FAILED or
                         (msg.type == ofp.OFPET_BAD_INSTRUCTION and msg.code == ofp.OFPBIC_UNSUP_INST))
        now = time.time()
        if now - self._of_error_last_log.get(key, 0) >= 10:
            self._of_error_last_log[key] = now
            LOG.warning(f"[OFError] dpid={dp.id} type={msg.type} code={msg.code} "
                        f"count={self.of_error_counts[key]} data_len={len(msg.data) if msg.data else 0}")
        if meter_related and self.meter_manager is not None:
            self.meter_manager.mark_error(dp.id, msg.type, msg.code)

    # ---- energy JSONL writer (one greenlet, batched blocking I/O, size rotation) ----
    _ENERGY_STREAMS = {
        'flow_removed': ('data/flows', 'flow_removed'),
        'agent1_cycle': ('data/energy', 'agent1_cycle'),
    }

    def _energy_enqueue(self, stream, record):
        if stream in self._energy_writer_disabled:
            return
        try:
            self._energy_writer_q.put_nowait((stream, record))
        except eventlet_queue.Full:
            self.lifecycle_stats['writer_dropped'] += 1

    def _energy_stream_path(self, stream):
        path = self._energy_writer_paths.get(stream)
        if path is None:
            d, base = self._ENERGY_STREAMS[stream]
            os.makedirs(d, exist_ok=True)
            path = os.path.join(d, f"{base}_{self.file_timestamp}.jsonl")
            self._energy_writer_paths[stream] = path
        return path

    def _energy_writer(self):
        max_bytes = int(setting.ENERGY_LOG_MAX_MB * 1024 * 1024)
        rotated = {}
        while True:
            try:
                first = self._energy_writer_q.get()
            except Exception:
                hub.sleep(1)
                continue
            if first is None:
                break
            batch = [first]
            while len(batch) < 500:
                try:
                    item = self._energy_writer_q.get_nowait()
                except eventlet_queue.Empty:
                    break
                if item is None:
                    break
                batch.append(item)
            by_stream = {}
            for stream, rec in batch:
                by_stream.setdefault(stream, []).append(rec)
            for stream, recs in by_stream.items():
                if stream in self._energy_writer_disabled:
                    continue
                try:
                    path = self._energy_stream_path(stream)
                    try:
                        if os.path.getsize(path) > max_bytes:
                            n = rotated.get(stream, 0) + 1
                            rotated[stream] = n
                            os.replace(path, f"{path}.{n}")
                    except OSError:
                        pass
                    with open(path, 'a') as f:
                        for rec in recs:
                            f.write(json.dumps(rec, separators=(',', ':'), default=str))
                            f.write('\n')
                except Exception as e:
                    self.lifecycle_stats['writer_errors'] += 1
                    LOG.error(f"[energy-writer] stream {stream} disabled after error: {e}")
                    self._energy_writer_disabled.add(stream)
            hub.sleep(0)

    # ------------------------------------------------------------------ #
    # Agent1 energy cycle                                                  #
    # ------------------------------------------------------------------ #
    def _update_paths_probability_agent1(self):
        """
        Ask the PPO agent for new path probabilities from the current path statistics
        (agent1_path_stats, the private copy of the latest snapshot) and rebind
        agent1_path_probabilities (sorted descending), Path_db_probability and the
        per-pair top-N table the workers draw from.
        """
        if self.agent1_logging:
            LOG.info("[Agent 1][Path Probability] INFO: Updating path probabilities using Agent1")
        self.agent1_timestamp = time.time()

        try:
            start_time = time.time()
            path_probs = self.agent1.select_action(self.agent1_path_stats)
            end_time = time.time()
            self.agent1_ready = True
            if self.agent1_logging:
                LOG.info(f"[Agent 1][Path Probability] INFO: Agent1 path probabilities calculated in {end_time - start_time:.2f} seconds")
            self.agent1_path_probabilities = dict(sorted(path_probs.items(),
                                                          key=lambda x: x[1], reverse=True))
            if self.agent1_logging:
                LOG.info(f"agent1_path_probabilities: {self.agent1_path_probabilities}")

            path_db_prob = {}
            for pathid, prob in self.agent1_path_probabilities.items():
                path = self.path_db[pathid]
                simple_path = [node[0] for node in path]
                path_db_prob[pathid] = {"probability": prob,
                                        "path_stats": self.agent1_path_stats[pathid],
                                        "path": simple_path}

            self.Path_db_probability = path_db_prob
            # Per-pair top-N built ONCE per cycle from the sorted probabilities and REBOUND:
            # the workers' path-cache misses become one dict lookup instead of a full-key scan.
            self.pair_top_paths = self._build_pair_top_paths(self.agent1_path_probabilities, path_db_prob)

        except Exception as e:
            LOG.error(f"Error updating path probabilities: {e}")

    def _update_agent1_with_rewards(self, rewards, state):
        """
        Feed one cycle's rewards to the PPO agent. `rewards` = {path_key: reward} computed
        from the FRESH snapshot s_t; `state` = private copy of s_t. PPO pairs its stored
        prev_path_stats (s_{t-1}, set by the previous select_action) with s_t as next_state.
        Called BEFORE select_action(s_t) in every cycle. Saves a checkpoint at most once
        every 300 s.
        """
        if self.agent1_logging:
            LOG.info("[Agent 1][Reward] INFO: Updating Agent1 with rewards")

        if not rewards:
            if self.agent1_logging:
                LOG.info('[Agent 1][Reward] INFO: no rewards this cycle')
            return

        try:
            self.agent1.update(rewards, state)
            if self.agent1_logging:
                LOG.info(f"[Agent 1][Reward] INFO: Updated Agent1 with {len(rewards)} rewards")

            if setting.AGENT1_SAVE_MODEL and time.time() - self._last_model_save >= 300:
                self._last_model_save = time.time()
                os.makedirs("models", exist_ok=True)
                self.agent1.save_model(AGENT1_MODEL_PATH)
                LOG.info(f"[Forwarding][Agent 1] Saved Agent1 model to {AGENT1_MODEL_PATH}")

        except Exception as e:
            LOG.error(f"Error updating Agent1 with rewards: {e}")

    def _compute_rewards(self, snap):
        """
        {path_key: reward} for this cycle plus the per-path term breakdown:
        energy_reward.hybrid_rewards over the paths that carried a forward flow within
        AGENT1_ACTIVITY_WINDOW_S (ledger), or all paths when AGENT1_REWARD_ALL_PATHS /
        after AGENT1_EMPTY_REWARD_FALLBACK_CYCLES empty cycles.
        """
        keys = None
        if not setting.AGENT1_REWARD_ALL_PATHS:
            active = self.get_paths_with_activity(setting.AGENT1_ACTIVITY_WINDOW_S)
            if active:
                self._empty_reward_cycles = 0
                keys = active
            else:
                self._empty_reward_cycles += 1
                if self._empty_reward_cycles >= max(1, int(setting.AGENT1_EMPTY_REWARD_FALLBACK_CYCLES)):
                    self._empty_reward_cycles = 0
                    keys = None          # fallback: reward every path once
                    if self.agent1_logging:
                        LOG.info("[Agent 1] no active paths for several cycles -> rewarding all paths once")
                else:
                    return {}, {}
        w = setting.AGENT1_REWARD_PATH_WEIGHT
        pmode = setting.AGENT1_PROP_POWER_MODE
        scale = setting.AGENT1_PATH_TERM_SCALE
        rewards = energy_reward.hybrid_rewards(snap, keys, w, pmode, scale)
        terms = {}
        if len(rewards) <= 500:
            for pk in rewards:
                pe = snap.paths.get(pk)
                if pe is not None:
                    terms[pk] = energy_reward.reward_terms(pe, snap.network, w, pmode, scale)
        return rewards, terms

    def _refresh_bw_limits(self, snap):
        """
        Recompute the per-path BW limit table ONCE per cycle from the fresh probabilities
        (Path_db_probability), the snapshot's per-path MLU and the ledger's live flow counts;
        REBIND path_bw_limit (workers only .get(); the previous table seeds the EWMA).
        Computed even in BW_LIMIT_DRY_RUN so the limits are exported.
        """
        if self.meter_manager is None or not self.Path_db_probability:
            return
        probs = {}
        mlus = {}
        for pk, v in self.Path_db_probability.items():
            probs[pk] = float(v.get('probability', 0.0) or 0.0)
            pe = snap.paths.get(pk)
            mlus[pk] = pe.mlu if pe is not None else 0.0
        n_active = self.get_active_flow_count_by_path()
        limits = bw_limit.compute_all_limits(probs, mlus, n_active, self.path_bw_limit, self.bw_cfg)
        self.path_bw_limit = limits          # REBIND
        summary = bw_limit.summarize(limits, self.bw_cfg)
        summary['ts'] = time.time()
        summary['dry_run'] = bool(setting.BW_LIMIT_DRY_RUN)
        summary['n_paths_with_flows'] = len(n_active)
        self.bw_limit_stats = summary

    def _log_agent1_cycle(self, snap, rewards, terms):
        if not setting.ENABLE_ENERGY_JSONL:
            return
        vals = list(rewards.values())
        rec = {
            'cycle': snap.cycle, 'ts': round(snap.ts, 3), 'agent1_cycle': self.agent1_cycle_count,
            'network': dict(snap.network._asdict()),
            'reward': {
                'n': len(vals), 'mean': (sum(vals) / len(vals)) if vals else None,
                'min': min(vals) if vals else None, 'max': max(vals) if vals else None,
                'w': setting.AGENT1_REWARD_PATH_WEIGHT, 'prop_mode': setting.AGENT1_PROP_POWER_MODE,
                'scale': setting.AGENT1_PATH_TERM_SCALE, 'all_paths': bool(setting.AGENT1_REWARD_ALL_PATHS),
            },
            'bw_limits': dict(self.bw_limit_stats),
            'ledger': self.get_ledger_counts(),
            'meters': self.meter_manager.count() if self.meter_manager is not None else 0,
            'terms': terms,
        }
        self._energy_enqueue('agent1_cycle', rec)

    def _agent1_cycle_once(self):
        """
        One Agent1 cycle, temporally aligned:
          snapshot s_t (immutable, from network_monitor)  ->  rewards_t from s_t
          -> agent1.update(rewards_t, copy(s_t))   [pairs prev s_{t-1} with s_t]
          -> select_action(copy(s_t))              [new probabilities, Path_db_probability]
          -> path cache clear -> per-path BW limits -> JSONL record.
        """
        get_snap = getattr(self.monitor, 'get_energy_snapshot', None)
        snap = get_snap() if get_snap is not None else None
        if snap is None or not snap.states:
            if self.agent1_logging:
                LOG.info('[Agent 1] No energy snapshot available yet, skipping')
            return
        if self._last_energy_cycle == snap.cycle:
            return                           # never train twice on the same s_t
        self._last_energy_cycle = snap.cycle
        self.agent1_cycle_count += 1

        state_t = dict(snap.states)          # private copy (tuples are immutable)
        self.agent1_path_stats = state_t

        rewards, terms = self._compute_rewards(snap)
        self.agent1_reward = rewards
        self.agent1_reward_terms = terms

        # 1) learn from the cycle that just ended (prev state s_{t-1} -> s_t)
        self._update_agent1_with_rewards(rewards, dict(state_t))
        # 2) new probabilities for the coming cycle (select_action stores its own copy)
        self._update_paths_probability_agent1()
        # 3) workers pick up the new probabilities
        self.worker_pool.path_cache.clear()
        # 4) per-path BW limits from the new probabilities + fresh MLU + live flow counts
        try:
            self._refresh_bw_limits(snap)
        except Exception as e:
            LOG.error(f"[BW] refreshing per-path limits failed: {e}")
        # 5) record
        self._log_agent1_cycle(snap, rewards, terms)

    def _path_probability_updater(self):
        """
        Dedicated Agent1 greenlet: every AGENT_UPDATE_INTERVAL run one energy cycle
        (_agent1_cycle_once). Skips cycles for which network_monitor has not published a
        new EnergySnapshot.
        """
        while True:
            hub.sleep(setting.AGENT_UPDATE_INTERVAL)
            try:
                self._agent1_cycle_once()
            except Exception as e:
                LOG.error(f"Error in path probability updater thread: {e}")

    def check_agent_device(self):
        """Return the device (cpu / cuda) the agent's network lives on."""
        try:
            return str(next(self.agent1.actor_critic.parameters()).device)
        except Exception:
            return "error"

    # ------------------------------------------------------------------ #
    # Shutdown                                                             #
    # ------------------------------------------------------------------ #
    def _cleanup(self):
        """
        Bounded shutdown (Ctrl+C): stop producing, kill our greenlets, flush the writer
        briefly, save the model, clear state. Per-switch meter deletes only when
        CLEAN_METERS_ON_SHUTDOWN (capped at METER_SHUTDOWN_DEADLINE_S): the join-time
        OFPM_ALL reset wipes every switch at the next start anyway.
        """
        t0 = time.monotonic()
        self._shutting_down = True
        LOG.info("Cleaning up Forwarding app resources...")

        try:
            self.worker_pool.stop()
        except Exception as e:
            LOG.error(f"Error stopping worker pool: {e}")
        if self.path_prob_thread is not None:
            try:
                self.path_prob_thread.kill()
            except Exception as e:
                LOG.error(f"Error stopping path probability thread: {e}")

        mm = self.meter_manager
        if mm is not None and setting.CLEAN_METERS_ON_SHUTDOWN:
            dps = list(self.monitor.datapaths_table.values())

            def _teardown():
                for dp in dps:
                    try:
                        mm.reset(dp.id, datapath=dp)
                    except Exception as e:
                        LOG.error(f"[BW] meter teardown failed for dpid {dp.id}: {e}")
            gt = hub.spawn(_teardown)
            deadline = time.monotonic() + float(setting.METER_SHUTDOWN_DEADLINE_S)
            while not getattr(gt, 'dead', True) and time.monotonic() < deadline:
                hub.sleep(0.05)
            if not getattr(gt, 'dead', True):
                gt.kill()
                LOG.warning("[BW] meter teardown cut short at METER_SHUTDOWN_DEADLINE_S")

        # Writer: poison pill, short drain, kill.
        if self._energy_writer_thread is not None:
            try:
                self._energy_writer_q.put_nowait(None)
                hub.sleep(0.2)
                self._energy_writer_thread.kill()
            except Exception as e:
                LOG.error(f"Error stopping energy writer: {e}")

        if setting.AGENT1_SAVE_MODEL and self.path_prob_thread is not None:
            try:
                os.makedirs("models", exist_ok=True)
                self.agent1.save_model(AGENT1_MODEL_PATH)
                LOG.info(f"Final agent model saved to {AGENT1_MODEL_PATH}")
            except Exception as e:
                LOG.error(f"Error saving final agent model: {e}")

        for attr_name in ('mptcp_flows', 'mptcp_client_ep_index', 'mptcp_token_ep',
                          'Path_db_probability', 'pair_top_paths', 'agent1_path_stats',
                          'agent1_path_probabilities', 'agent1_reward', 'agent1_reward_terms',
                          'flow_ledger', 'path_active_flows', 'path_active_flows_rev',
                          'path_activity_ts_fwd'):
            getattr(self, attr_name).clear()
        self.mptcp_flows_cookie = setting.COOKIE_ID
        self.tcp_flows_cookie = setting.COOKIE_ID
        self.path_bw_limit = {}
        LOG.info(f"[Forwarding] cleanup done in {time.monotonic() - t0:.2f}s")

    def close(self):
        """
        Close the application and clean up resources.
        This function is called when the application is shutting down.
        """
        try:
            self._cleanup()
            LOG.info("Forwarding app closed")
        except Exception as e:
            LOG.error(f"Error during application close: {e}")
        finally:
            super(Forwarding, self).close()
