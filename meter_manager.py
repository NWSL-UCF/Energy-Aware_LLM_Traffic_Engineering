"""Per-switch OpenFlow 1.3 meter POOL for the energy-aware BW limiter.

One meter per LIVE flow / MPTCP subflow on its ingress switch, but meters are
RE-USED, never deleted per flow:

  * ``allocate``  hands the flow a meter - an idle one with the same rate (no
    message at all), an idle one with another rate (one ``OFPMC_MODIFY``), or a
    fresh id (one ``OFPMC_ADD``);
  * ``release``   (on the switch's FlowRemoved) only moves the meter back to the
    idle pool - NO message, nothing to block on;
  * ``reset``     (switch join, ``OFPM_ALL`` delete when clean_on_join) is the only
    deletion.

A meter is referenced by at most one live flow at a time, so the per-flow cap is
unchanged; rates are quantised (``BW_QUANTUM_Mbps``) so re-use hits are the norm
and per-flow MeterMod traffic is ~0 in steady state.

Concurrency: ``allocate`` writes its bookkeeping BEFORE ``datapath.send_msg``
(send_msg may yield under eventlet); ``release`` never sends. Messages are built
through ``datapath.ofproto_parser`` so any datapath-like object with ``id``,
``ofproto``, ``ofproto_parser`` and ``send_msg`` works; ``release`` accepts a
bare dpid as well.
"""
import logging
import time

_STAT_KEYS = ("added", "reused", "modified", "released", "release_miss", "exhausted",
              "disabled_skips", "resets", "forgets", "errors", "dry_run_skips")


def _band_for(rate_mbps, burst_fraction):
    """rate_kbps = max(1, int(rate_mbps*1000)); burst_kbits = max(1, int(rate_kbps*burst_fraction))."""
    rate_kbps = max(1, int(float(rate_mbps) * 1000))
    burst = max(1, int(rate_kbps * float(burst_fraction)))
    return rate_kbps, burst


class MeterManager(object):
    """Meter pool + lifecycle for per-flow drop meters, keyed by (dpid, cookie)."""

    def __init__(self, max_meter_id, burst_fraction, clean_on_join, error_backoff_s,
                 logger=None, dry_run=False):
        self.max_meter_id = int(max_meter_id)
        self.burst_fraction = float(burst_fraction)
        self.clean_on_join = bool(clean_on_join)
        self.error_backoff_s = float(error_backoff_s)
        self.dry_run = bool(dry_run)
        self.logger = logger or logging.getLogger("MeterManager")

        self._next_id = {}          # dpid -> next fresh meter id (starts at 1)
        self.busy = {}              # (dpid, cookie)   -> meter_id      (meters held by a live flow)
        self.meter_rate = {}        # (dpid, meter_id) -> rate_kbps     (busy AND idle meters)
        self.free_by_rate = {}      # dpid -> {rate_kbps: [meter_id, ...]}  (idle meters)
        self._n_busy = {}           # dpid -> live count   (O(1) count / snapshot)
        self._n_free = {}           # dpid -> idle count
        self.capacity = {}          # dpid -> max meter id usable (from MeterFeatures)
        self.disabled_until = {}    # dpid -> monotonic deadline of the error backoff
        self.stats = {k: 0 for k in _STAT_KEYS}
        self._exhausted_logged = set()

    # ------------------------------------------------------------------ gates
    @staticmethod
    def _dpid_of(datapath_or_dpid):
        return datapath_or_dpid if isinstance(datapath_or_dpid, int) else datapath_or_dpid.id

    def enabled_for(self, dpid):
        """True unless capacity[dpid]==0 (unmetered) or the error backoff is still running."""
        if self.capacity.get(dpid, None) == 0:
            return False
        deadline = self.disabled_until.get(dpid)
        if deadline is None:
            return True
        return time.monotonic() >= deadline

    def _limit_for(self, dpid):
        """Effective id ceiling = min(max_meter_id, capacity.get(dpid, max_meter_id))."""
        return min(self.max_meter_id, self.capacity.get(dpid, self.max_meter_id))

    def _take_fresh_id(self, dpid):
        """Next never-used id if <= ceiling; None when the id space is exhausted."""
        nxt = self._next_id.get(dpid, 1)
        if nxt > self._limit_for(dpid):
            return None
        self._next_id[dpid] = nxt + 1
        return nxt

    def _take_free(self, dpid, rate_kbps):
        """Pop an idle meter: exact rate first, else any rate. -> (meter_id, same_rate) or (None, False)."""
        pools = self.free_by_rate.get(dpid)
        if not pools:
            return None, False
        ids = pools.get(rate_kbps)
        if ids:
            mid = ids.pop()
            if not ids:
                del pools[rate_kbps]
            self._n_free[dpid] -= 1
            return mid, True
        for rate, ids in pools.items():
            if ids:
                mid = ids.pop()
                if not ids:
                    del pools[rate]
                self._n_free[dpid] -= 1
                return mid, False
        return None, False

    def _bind(self, dpid, cookie, meter_id):
        self.busy[(dpid, cookie)] = meter_id
        self._n_busy[dpid] = self._n_busy.get(dpid, 0) + 1

    # -------------------------------------------------------------- lifecycle
    def allocate(self, datapath, cookie, rate_mbps):
        """Give (dpid, cookie) a meter at rate_mbps and return [OFPInstructionMeter(meter_id)].

        * same cookie already holding a meter: nothing sent when the rate is unchanged,
          else OFPMC_MODIFY;
        * an idle meter with the same quantised rate: NO message (``reused``);
        * an idle meter with another rate: OFPMC_MODIFY (``modified``);
        * otherwise a fresh id: OFPMC_ADD (``added``).
        Returns [] when nothing should be metered (dry-run, backed-off / unmetered dpid,
        rate <= 0, id space exhausted). Bookkeeping is written BEFORE send_msg.
        band: rate_kbps = max(1, int(rate_mbps*1000)), burst = max(1, int(rate_kbps*burst_fraction)).
        """
        dpid = datapath.id
        if self.dry_run:
            self.stats["dry_run_skips"] += 1
            return []
        if not self.enabled_for(dpid):
            self.stats["disabled_skips"] += 1
            return []
        if rate_mbps is None or rate_mbps <= 0:
            return []

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        rate_kbps, burst = _band_for(rate_mbps, self.burst_fraction)
        key = (dpid, cookie)

        meter_id = self.busy.get(key)
        if meter_id is not None:
            if self.meter_rate.get((dpid, meter_id)) == rate_kbps:
                self.stats["reused"] += 1
                return [parser.OFPInstructionMeter(meter_id)]
            command = ofproto.OFPMC_MODIFY
            self.stats["modified"] += 1
        else:
            meter_id, same_rate = self._take_free(dpid, rate_kbps)
            if meter_id is not None:
                self._bind(dpid, cookie, meter_id)
                if same_rate:
                    self.stats["reused"] += 1
                    return [parser.OFPInstructionMeter(meter_id)]      # no message
                command = ofproto.OFPMC_MODIFY
                self.stats["modified"] += 1
            else:
                meter_id = self._take_fresh_id(dpid)
                if meter_id is None:
                    self.stats["exhausted"] += 1
                    if dpid not in self._exhausted_logged:
                        self._exhausted_logged.add(dpid)
                        self.logger.warning(
                            "MeterManager: dpid %s meter id space exhausted (limit %d); "
                            "installing unmetered", dpid, self._limit_for(dpid))
                    return []
                self._bind(dpid, cookie, meter_id)
                command = ofproto.OFPMC_ADD
                self.stats["added"] += 1

        self.meter_rate[(dpid, meter_id)] = rate_kbps
        band = parser.OFPMeterBandDrop(rate=rate_kbps, burst_size=burst)
        msg = parser.OFPMeterMod(datapath=datapath, command=command,
                                 flags=ofproto.OFPMF_KBPS | ofproto.OFPMF_BURST,
                                 meter_id=meter_id, bands=[band])
        datapath.send_msg(msg)  # LAST
        return [parser.OFPInstructionMeter(meter_id)]

    def release(self, datapath_or_dpid, cookie):
        """The flow of (dpid, cookie) is gone (FlowRemoved): return its meter to the idle
        pool. NO message is sent (the switch already removed the referencing entry).
        Returns the meter_id, or None (``release_miss``) for an unknown cookie."""
        dpid = self._dpid_of(datapath_or_dpid)
        key = (dpid, cookie)
        meter_id = self.busy.pop(key, None)
        if meter_id is None:
            self.stats["release_miss"] += 1
            return None
        self._n_busy[dpid] = self._n_busy.get(dpid, 1) - 1
        rate = self.meter_rate.get((dpid, meter_id), 0)
        self.free_by_rate.setdefault(dpid, {}).setdefault(rate, []).append(meter_id)
        self._n_free[dpid] = self._n_free.get(dpid, 0) + 1
        self.stats["released"] += 1
        return meter_id

    def _drop_dpid(self, dpid):
        """Forget every meter of dpid (busy and idle) and restart its id space at 1."""
        for key in [k for k in self.busy if k[0] == dpid]:
            del self.busy[key]
        for key in [k for k in self.meter_rate if k[0] == dpid]:
            del self.meter_rate[key]
        self.free_by_rate.pop(dpid, None)
        self._n_busy[dpid] = 0
        self._n_free[dpid] = 0
        self._next_id[dpid] = 1
        self._exhausted_logged.discard(dpid)

    def reset(self, dpid, datapath=None):
        """Switch (re)joined: wipe bookkeeping, ids restart at 1; when datapath is
        given and clean_on_join, send OFPMeterMod(DELETE, meter_id=OFPM_ALL) - the
        only deletion in the lifecycle."""
        self._drop_dpid(dpid)
        self.stats["resets"] += 1
        if datapath is not None and self.clean_on_join:
            parser = datapath.ofproto_parser
            ofproto = datapath.ofproto
            msg = parser.OFPMeterMod(datapath=datapath, command=ofproto.OFPMC_DELETE,
                                     flags=0, meter_id=ofproto.OFPM_ALL, bands=[])
            datapath.send_msg(msg)

    def forget(self, dpid):
        """Switch left: drop bookkeeping for dpid without sending anything."""
        self._drop_dpid(dpid)
        self.stats["forgets"] += 1

    def set_capacity(self, dpid, max_meter):
        """capacity[dpid] = int(max_meter) from MeterFeatures; 0 => permanently unmetered."""
        self.capacity[dpid] = int(max_meter)

    def mark_error(self, dpid, err_type=None, err_code=None):
        """Meter-related OFPErrorMsg: disabled_until[dpid] = now + error_backoff_s."""
        self.disabled_until[dpid] = time.monotonic() + self.error_backoff_s
        self.stats["errors"] += 1
        self.logger.warning("MeterManager: dpid %s meter error type=%s code=%s; "
                            "unmetered for %.0fs", dpid, err_type, err_code,
                            self.error_backoff_s)

    # ---------------------------------------------------------------- lookups
    def count(self, dpid=None):
        """Live (busy) meters on dpid (all dpids when None). O(1)."""
        if dpid is None:
            return sum(self._n_busy.values())
        return self._n_busy.get(dpid, 0)

    def free_count(self, dpid=None):
        """Idle meters on dpid (all dpids when None). O(1)."""
        if dpid is None:
            return sum(self._n_free.values())
        return self._n_free.get(dpid, 0)

    # --------------------------------------------------------------- snapshot
    def snapshot(self):
        """{"per_dpid": {dpid: {busy, free, n_rates, next_id, capacity, disabled_for_s}},
        "total_busy", "total_free", "stats" (+ "allocated" = added+reused+modified), "dry_run"}."""
        now = time.monotonic()
        dpids = set(self._next_id) | set(self.capacity) | set(self.disabled_until) | \
            set(self._n_busy) | set(self._n_free)
        per_dpid = {}
        for dpid in sorted(dpids):
            deadline = self.disabled_until.get(dpid)
            per_dpid[dpid] = {
                "busy": self._n_busy.get(dpid, 0),
                "free": self._n_free.get(dpid, 0),
                "n_rates": len(self.free_by_rate.get(dpid, {})),
                "next_id": self._next_id.get(dpid, 1),
                "capacity": self.capacity.get(dpid, self.max_meter_id),
                "disabled_for_s": max(0.0, deadline - now) if deadline is not None else 0.0,
            }
        stats = dict(self.stats)
        stats["allocated"] = stats["added"] + stats["reused"] + stats["modified"]
        return {
            "per_dpid": per_dpid,
            "total_busy": self.count(),
            "total_free": self.free_count(),
            "stats": stats,
            "dry_run": self.dry_run,
        }
