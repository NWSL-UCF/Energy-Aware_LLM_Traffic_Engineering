"""
Green thread worker pool for high-rate packet_in processing.

Architecture:
    Ryu Event Loop
        |
        packet_in_handler() -> dispatch(ev)  [thin dispatcher, returns immediately]
        |
        eventlet.Queue (packet_queue)
        |
        Worker_0 ... Worker_N  [green threads: parse -> install_flow (PRIORITY_TCP)]
        |                   |
        |              [tcp.option==30? -> dispatch_mptcp(payload)]
        |                   |
        [TCP/UDP]      eventlet.Queue (mptcp_queue, payload=parsed pkt+ev)
        |                   |
        agent_queue    MPTCP Worker (single, serial)
        |                   |
        Agent Thread   -> agent_queue      [SINGLE WRITER of Forwarding.flow_ledger:
        |                                    flow_installed / mptcp_track_cookie -> ledger_add,
        |                                    flow_removed (from EventOFPFlowRemoved) ->
        |                                    process_flow_removed (pool bookkeeping only, no send),
        |                                    dpid_gone (switch left) -> ledger_drop_dpid]

All threads are eventlet green threads (cooperative multitasking).
datapath.send_msg() and hub.Semaphore() are safe in this model.
MPTCP is serialized to a single worker to eliminate race conditions
on shared MPTCP state (mptcp_flows, cookie IDs, etc.).
"""

import time
import logging
import random as _random
import traceback
from ryu.lib import hub
from eventlet import queue as eventlet_queue

import setting

LOG = logging.getLogger('ryu.app.packet_in_worker_pool')


class InstallDedup:
    """
    Duplicate-install guard keyed by the flow 5-tuple.

    record() is called right after path selection (BEFORE the FlowMods go out) with the
    ingress dpid and its out_port; lookup() within `window` seconds returns that entry so
    a retransmitted SYN (1/3/7 s) racing the not-yet-landed FlowMods, or a second worker
    holding the same flow, is answered with ONE packet-out and never re-installed (a
    second install would silently replace the entry and orphan its meter).
    Green-thread-safe: plain dict ops, no yield points.
    """

    def __init__(self, window=10.0):
        self._last = {}    # 5-tuple -> (ts, ingress_dpid, out_port)
        self._window = float(window)

    def lookup(self, key, now=None):
        """(ts, dpid, out_port) if key was recorded less than `window` s ago, else None."""
        entry = self._last.get(key)
        if entry is None:
            return None
        now = time.time() if now is None else now
        if now - entry[0] >= self._window:
            del self._last[key]
            return None
        return entry

    def record(self, key, dpid, out_port, now=None):
        self._last[key] = (time.time() if now is None else now, dpid, out_port)

    def forget(self, key):
        self._last.pop(key, None)

    def cleanup(self, max_age=60.0):
        """Remove entries older than max_age seconds."""
        now = time.time()
        stale = [k for k, v in self._last.items() if now - v[0] > max_age]
        for k in stale:
            del self._last[k]

    def size(self):
        return len(self._last)


class PathCache:
    """
    Cache of pre-computed path selections (top N paths per switch pair).

    Key: (src_dpid, dst_dpid, strategy)
    Value: (top_paths_list, timestamp)
        where top_paths_list = [(path_key, path, probability), ...]

    get() returns a uniform random pick from the cached top-N paths: the
    probabilities only decide which paths make the top-N set, the pick within
    the set is uniform, so traffic spreads evenly across the set instead of
    piling on the highest-probability path.

    Workers do an O(1) dict lookup + O(1) random pick. The agent cycle calls
    clear() after new probabilities are computed; entries also expire after
    `ttl` seconds. Green-thread-safe: plain dict ops, no yield points.
    """

    def __init__(self, ttl=10.0):
        """
        Args:
            ttl: Time-to-live in seconds for cached entries.
        """
        self._cache = {}  # (src_dpid, dst_dpid, strategy) -> (top_paths, timestamp)
        self._ttl = ttl

    def get(self, src_dpid, dst_dpid, strategy='default'):
        """
        Return uniform random (path_key, path) from cached top paths, or None.
        Each call may return a different path from the cached set.
        """
        key = (src_dpid, dst_dpid, strategy)
        entry = self._cache.get(key)
        if entry is None:
            return None
        top_paths, ts = entry
        if time.time() - ts > self._ttl:
            del self._cache[key]
            return None
        if not top_paths:
            return None
        chosen = _random.choice(top_paths)
        return (chosen[0], chosen[1])

    def put(self, src_dpid, dst_dpid, top_paths, strategy='default'):
        """Store top N paths with probabilities.

        Args:
            top_paths: list of (path_key, path, probability) tuples.
        """
        key = (src_dpid, dst_dpid, strategy)
        self._cache[key] = (top_paths, time.time())

    def clear(self):
        """Invalidate all cached entries."""
        self._cache.clear()

    def size(self):
        return len(self._cache)


class PacketInWorkerPool:
    """
    Green thread worker pool for packet_in processing.

    Manages:
    - packet_queue: eventlet.Queue fed by the thin packet_in_handler dispatcher
    - N green thread workers consuming from packet_queue
    - mptcp_queue: eventlet.Queue consumed by the single serial MPTCP worker
    - agent_queue: eventlet.Queue for worker -> agent thread reports
    - InstallDedup (duplicate-install guard) and PathCache instances
    - Processing counters (self.stats), exported by pool_stats()
    """

    def __init__(self, forwarding_app, num_workers=None):
        """
        Args:
            forwarding_app: Reference to the Forwarding RyuApp instance.
            num_workers: Number of worker green threads (default: setting.WORKER_POOL_SIZE).
        """
        self.app = forwarding_app
        self.num_workers = num_workers or setting.WORKER_POOL_SIZE

        # Queues
        self.packet_queue = eventlet_queue.Queue(maxsize=setting.PACKET_QUEUE_MAXSIZE)
        self.agent_queue = eventlet_queue.Queue(maxsize=setting.AGENT_QUEUE_MAXSIZE)
        self.mptcp_queue = eventlet_queue.Queue(maxsize=setting.MPTCP_QUEUE_MAXSIZE)

        # Duplicate-install guard and path cache
        self.dedup = (InstallDedup(window=setting.INSTALL_DEDUP_WINDOW_S)
                      if setting.ENABLE_INSTALL_DEDUP else None)
        self.path_cache = PathCache(ttl=setting.PATH_CACHE_TTL)

        # Thread handles
        self._workers = []
        self._agent_thread = None
        self._mptcp_thread = None
        self._cleanup_thread = None
        self._running = False

        # Counters (green-thread-safe: no yield points in increments). The queue-drop
        # counters are the only visibility of backpressure drops.
        self.stats = {
            'dispatched': 0,
            'dropped_queue_full': 0,
            'processed': 0,
            'errors': 0,
            'mptcp_dropped_queue_full': 0,
            'agent_reports_dropped': 0,  # reports lost because agent_queue was full (untracked cookie / lost flow_removed)
            'flow_removed_reports': 0,
        }

    def start(self):
        """Start the worker threads, the MPTCP worker, the agent thread and the cleanup thread."""
        if self._running:
            LOG.warning("PacketInWorkerPool already running")
            return

        self._running = True
        LOG.info(f"[WorkerPool] Starting with {self.num_workers} workers, "
                 f"packet_queue={setting.PACKET_QUEUE_MAXSIZE}, "
                 f"agent_queue={setting.AGENT_QUEUE_MAXSIZE}")

        for i in range(self.num_workers):
            t = hub.spawn(self._worker_loop, i)
            self._workers.append(t)

        self._agent_thread = hub.spawn(self._agent_loop)
        self._mptcp_thread = hub.spawn(self._mptcp_loop)
        self._cleanup_thread = hub.spawn(self._cleanup_loop)

        LOG.info("[WorkerPool] All threads started (including dedicated MPTCP worker)")

    def stop(self):
        """Stop all threads gracefully."""
        self._running = False
        LOG.info("[WorkerPool] Stopping...")

        # Poison pills to the workers, the agent thread and the MPTCP thread
        for _ in self._workers:
            try:
                self.packet_queue.put_nowait(None)
            except eventlet_queue.Full:
                pass
        for q in (self.agent_queue, self.mptcp_queue):
            try:
                q.put_nowait(None)
            except eventlet_queue.Full:
                pass

        # Kill whatever is still parked in a blocking wait
        for t in self._workers + [self._agent_thread, self._mptcp_thread, self._cleanup_thread]:
            if t is not None and hasattr(t, 'kill'):
                try:
                    t.kill()
                except Exception:
                    pass

        self._workers.clear()
        LOG.info("[WorkerPool] Stopped")

    def dispatch(self, ev):
        """
        Called by packet_in_handler (Ryu event loop).
        Enqueue the event for a worker. Returns immediately.

        If queue is full, the event is dropped (backpressure).
        """
        self.stats['dispatched'] += 1
        try:
            self.packet_queue.put_nowait(ev)
        except eventlet_queue.Full:
            self.stats['dropped_queue_full'] += 1

    def dispatch_mptcp(self, payload):
        """
        Dispatch an MPTCP packet to the dedicated serial MPTCP worker.
        Non-blocking: drops if queue is full (backpressure).

        Payload is a dict carrying the already-parsed packet so the MPTCP
        worker does not re-parse: {'ev', 'pkt', 'option'}.
        """
        try:
            self.mptcp_queue.put_nowait(payload)
        except eventlet_queue.Full:
            self.stats['mptcp_dropped_queue_full'] += 1

    def report_to_agent(self, report):
        """
        Non-blocking enqueue of a report dict to the agent thread. Returns True when
        queued, False when the queue was full (the caller may then act inline).

        Report formats:
        {
            'type': 'flow_installed' | 'mptcp_track_cookie',
            'cookie': int, 'src_dpid': int (ingress dpid), 'path_key': str,
            'direction': 'fwd' | 'rev', 'kind': 'tcp' | 'mptcp', 'bw_limit': float or None,
            'timestamp': float,
        }
        {
            'type': 'flow_removed',
            'dpid': int, 'cookie': int, 'byte_count', 'packet_count', 'duration_sec',
            'reason': int (OFPRR_*), 'priority', 'idle_timeout', 'ts': float,
        }
        {
            'type': 'dpid_gone', 'dpid': int, 'ts': float,
        }
        """
        try:
            self.agent_queue.put_nowait(report)
            return True
        except eventlet_queue.Full:
            # A dropped flow_installed leaves an untracked cookie (its meter is still
            # released by FlowRemoved); a dropped flow_removed leaves a stale ledger
            # entry. Count it - this is the main invisible failure.
            self.stats['agent_reports_dropped'] += 1
            return False

    def pool_stats(self):
        """JSON-serialisable snapshot of the counters and queue depths (for REST)."""
        return {
            'counters': dict(self.stats),
            'packet_queue': self.packet_queue.qsize(),
            'mptcp_queue': self.mptcp_queue.qsize(),
            'agent_queue': self.agent_queue.qsize(),
            'dedup_entries': self.dedup.size() if self.dedup is not None else 0,
            'path_cache_entries': self.path_cache.size(),
            'workers': self.num_workers,
        }

    # ---- Internal loops ----

    def _worker_loop(self, worker_id):
        """Main loop for a worker green thread."""
        LOG.info(f"[WorkerPool] Worker {worker_id} started")
        while self._running:
            try:
                ev = self.packet_queue.get(timeout=1.0)
                if ev is None:  # Poison pill
                    break
                self.app._process_packet_in_worker(ev, self)
                self.stats['processed'] += 1
            except eventlet_queue.Empty:
                continue  # Timeout, check _running flag
            except Exception as e:
                self.stats['errors'] += 1
                LOG.error(f"[WorkerPool] Worker {worker_id} error: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        LOG.info(f"[WorkerPool] Worker {worker_id} stopped")

    def _agent_loop(self):
        """
        Dedicated agent green thread.
        Consumes reports from agent_queue and processes them.
        """
        LOG.info("[WorkerPool] Agent report thread started")
        while self._running:
            try:
                report = self.agent_queue.get(timeout=1.0)
                if report is None:  # Poison pill
                    break
                self._handle_agent_report(report)
            except eventlet_queue.Empty:
                continue
            except Exception as e:
                LOG.error(f"[WorkerPool] Agent thread error: {e}")
        LOG.info("[WorkerPool] Agent report thread stopped")

    def _mptcp_loop(self):
        """
        Dedicated MPTCP green thread.
        Hands each queued payload to Forwarding._process_mptcp_worker(payload)
        serially, so there are no races on the shared MPTCP state (mptcp_flows,
        cookie IDs). Also runs the periodic cleanup of stale MPTCP state.
        """
        LOG.info("[WorkerPool] MPTCP worker started (serial processing)")
        last_cleanup = time.time()
        while self._running:
            try:
                payload = self.mptcp_queue.get(timeout=1.0)
                if payload is None:  # Poison pill
                    break
                self.app._process_mptcp_worker(payload)
            except eventlet_queue.Empty:
                pass  # Timeout, check _running flag and run cleanup
            except Exception as e:
                self.stats['errors'] += 1
                LOG.error(f"[WorkerPool] MPTCP worker error: {type(e).__name__}: {e}\n{traceback.format_exc()}")

            # Periodic cleanup of stale MPTCP state
            now = time.time()
            if now - last_cleanup > setting.MPTCP_CLEANUP_INTERVAL:
                try:
                    self.app._cleanup_mptcp_state()
                except Exception as e:
                    LOG.error(f"[WorkerPool] MPTCP cleanup error: {e}")
                last_cleanup = now
        LOG.info("[WorkerPool] MPTCP worker stopped")

    def _handle_agent_report(self, report):
        """
        Process a single report. This greenlet is the SINGLE WRITER of the Forwarding
        flow ledger (ledger_add / process_flow_removed / ledger_drop_dpid), so no lock
        is needed there.
        """
        report_type = report.get('type')

        if report_type in ('flow_installed', 'mptcp_track_cookie'):
            cookie = report.get('cookie')
            src_dpid = report.get('src_dpid')
            path_key = report.get('path_key')
            if cookie is None or path_key is None:
                return
            kind = report['kind']
            try:
                self.app.ledger_add(cookie, path_key, report.get('direction', 'fwd'), src_dpid,
                                    kind, report.get('bw_limit'), report.get('timestamp'))
            except Exception as e:
                LOG.error(f"[WorkerPool] Error recording cookie {cookie} in the flow ledger: {e}")

        elif report_type == 'flow_removed':
            self.stats['flow_removed_reports'] += 1
            try:
                self.app.process_flow_removed(report)
            except Exception as e:
                LOG.error(f"[WorkerPool] Error processing flow_removed for cookie {report.get('cookie')}: {e}")

        elif report_type == 'dpid_gone':
            try:
                self.app.ledger_drop_dpid(report.get('dpid'))
            except Exception as e:
                LOG.error(f"[WorkerPool] Error dropping ledger entries of dpid {report.get('dpid')}: {e}")

    def _cleanup_loop(self):
        """Periodic cleanup of the dedup guard's stale entries."""
        while self._running:
            hub.sleep(30)
            if self.dedup is not None:
                self.dedup.cleanup(max_age=60.0)
