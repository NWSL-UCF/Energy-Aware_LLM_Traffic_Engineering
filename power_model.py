"""Pure linear link-power model and per-path / network energy aggregates.

No ryu imports: only stdlib + setting. network_monitor.py turns its link utilization
into a NetworkEnergy row and an EnergySnapshot once per cycle; energy_reward.py and
bw_limit.py use clamp01; forwarding.py reads the snapshot through the monitor.

Model (setting.POWER_CONSUMPTION_FACTOR = 40 by default):

    P_link(util) = clamp01(util) * POWER_CONSUMPTION_FACTOR
    P_path       = sum over MONITORED hops of P_link(util_hop)
    P_total      = sum over the FIXED monitored link set of P_link(util_link)
    P_max        = N_links * P_link(1.0)

``link_power`` is the ONE pluggable point: an idle term or a non-linear curve
goes there and nothing else changes.
"""
from collections import namedtuple
from types import MappingProxyType

import setting

# p_path: raw W over monitored hops; alu/mlu: mean/max util over monitored hops;
# n_hops: number of MONITORED hops (0 = "no data, skip this path").
PathEnergy = namedtuple("PathEnergy", "p_path alu mlu n_hops delay_rank")
# p_total: sum of link power over the fixed monitored set; p_max = n_links*link_power_max.
NetworkEnergy = namedtuple("NetworkEnergy", "p_total alu_net mlu_net p_max n_links ts")
# paths: MappingProxyType {path_key: PathEnergy};
# states: MappingProxyType {path_key: (p_norm, alu, mlu, delay_rank)}.
EnergySnapshot = namedtuple("EnergySnapshot", "cycle ts network paths states")


def clamp01(x):
    """min(1.0, max(0.0, float(x))); non-numeric / None -> 0.0."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    if v != v:  # NaN
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def link_key(src_dpid, dst_dpid):
    """Directed link key "s{src}:s{dst}" (same format as network_monitor.link_utilization_links)."""
    return "s%s:s%s" % (src_dpid, dst_dpid)


def link_power(util):
    """P_link = clamp01(util) * setting.POWER_CONSUMPTION_FACTOR.

    The ONE pluggable point of the power model.
    """
    return clamp01(util) * float(setting.POWER_CONSUMPTION_FACTOR)


def link_power_max():
    """Power of a fully loaded link: link_power(1.0)."""
    return link_power(1.0)


def monitored_link_keys(port_link_mapping, name_to_dpid, excluded_switch_dpids, excluded_ports):
    """Fixed set of directed link keys the monitor reports (the keys network_monitor's
    port-stats handler writes to link_utilization_links).

    port_link_mapping: {"Name:portN": dst_name}; name_to_dpid: {Name: dpid}.
    key = link_key(name_to_dpid[Name], name_to_dpid[dst_name]); entries that do not
    resolve are skipped.  Excluded: src_dpid in excluded_switch_dpids or
    (src_dpid, port_no) in excluded_ports.  Spine-leaf -> exactly 720 keys.
    """
    excluded_switch_dpids = set(excluded_switch_dpids or ())
    excluded_ports = set(excluded_ports or ())
    keys = set()
    for port_label, dst_name in (port_link_mapping or {}).items():
        try:
            switch_name, port_part = port_label.split(":", 1)
        except ValueError:
            continue
        src_dpid = name_to_dpid.get(switch_name)
        dst_dpid = name_to_dpid.get(dst_name)
        if src_dpid is None or dst_dpid is None:
            continue
        if src_dpid in excluded_switch_dpids:
            continue
        try:
            port_no = int(port_part.replace("port", ""))
        except ValueError:
            port_no = None
        if port_no is not None and (src_dpid, port_no) in excluded_ports:
            continue
        keys.add(link_key(src_dpid, dst_dpid))
    return frozenset(keys)


def path_energy(dpids, util, monitored, delay_rank=0.0):
    """PathEnergy over the MONITORED hops (dpids[i], dpids[i+1]) of a path.

    u_h = clamp01(util.get(key, 0.0)); p_path = sum link_power(u_h);
    alu = mean(u_h); mlu = max(u_h); n_hops = number of monitored hops.
    n_hops == 0 -> PathEnergy(0, 0, 0, 0, delay_rank).
    """
    util = util or {}
    utils = []
    for i in range(len(dpids) - 1):
        key = link_key(dpids[i], dpids[i + 1])
        if key not in monitored:
            continue
        utils.append(clamp01(util.get(key, 0.0)))
    n = len(utils)
    if n == 0:
        return PathEnergy(0.0, 0.0, 0.0, 0, delay_rank)
    p_path = sum(link_power(u) for u in utils)
    return PathEnergy(float(p_path), sum(utils) / n, max(utils), n, delay_rank)


def path_power_norm(pe):
    """P_norm = p_path / (n_hops * link_power_max()); 0.0 when n_hops == 0.

    Under the linear model this equals ALU_p (documented, by design).
    """
    if not pe.n_hops:
        return 0.0
    denom = pe.n_hops * link_power_max()
    if denom <= 0:
        return 0.0
    return clamp01(pe.p_path / denom)


def network_energy(util, monitored, ts):
    """NetworkEnergy over the FIXED monitored set (missing key -> util 0.0).

    p_total = sum link_power(u); alu_net = mean(u); mlu_net = max(u);
    p_max = n_links * link_power_max(); n_links = len(monitored); ts = caller's cycle timestamp.
    """
    util = util or {}
    n = len(monitored)
    if n == 0:
        return NetworkEnergy(0.0, 0.0, 0.0, 0.0, 0, ts)
    total_u = 0.0
    max_u = 0.0
    p_total = 0.0
    for key in monitored:
        u = clamp01(util.get(key, 0.0))
        total_u += u
        if u > max_u:
            max_u = u
        p_total += link_power(u)
    return NetworkEnergy(float(p_total), total_u / n, max_u,
                         n * link_power_max(), n, ts)


def make_snapshot(cycle, ts, network, paths, states):
    """EnergySnapshot with paths/states wrapped read-only (MappingProxyType over a copy)."""
    return EnergySnapshot(cycle, ts, network,
                          MappingProxyType(dict(paths or {})),
                          MappingProxyType(dict(states or {})))
