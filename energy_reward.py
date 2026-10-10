"""Pure energy-aware reward for Agent1 (hybrid per-path + network objective).

    prop_p       = 'relative': (P_p/n_hops_p) / (P_total/N_links)   'raw': P_p/P_total
    path_cost_p  = clamp01( scale * (1-ALU_p) * MLU_p * prop_p )
    network_cost = (1-ALU_net) * MLU_net * (P_total/P_max)
    cost_p       = w * path_cost_p + (1-w) * network_cost
    reward_p     = 1 - clamp01(cost_p)

Properties of the formula: under the linear power model prop_p == ALU_p/ALU_net
and POWER_CONSUMPTION_FACTOR cancels out of every term; (1-ALU)*MLU is
non-monotonic in load (a saturated path has cost -> 0), i.e. the objective
rewards consolidation onto full links.
"""
from power_model import clamp01


def proportional_power(pe, net, mode="relative"):
    """relative: (pe.p_path/pe.n_hops) / (net.p_total/net.n_links); raw: pe.p_path/net.p_total.

    0.0 when p_total <= 0, n_hops == 0 (relative) or n_links == 0 (relative).
    Unknown mode -> ValueError.
    """
    if net.p_total <= 0:
        return 0.0
    if mode == "relative":
        if not pe.n_hops or not net.n_links:
            return 0.0
        return (pe.p_path / pe.n_hops) / (net.p_total / net.n_links)
    if mode == "raw":
        return pe.p_path / net.p_total
    raise ValueError("unknown proportional_power mode: %r" % (mode,))


def _path_cost_from_prop(pe, prop, scale):
    """clamp01(scale * (1-alu) * mlu * prop)."""
    return clamp01(scale * (1.0 - pe.alu) * pe.mlu * prop)


def path_cost(pe, net, mode="relative", scale=1.0):
    """clamp01(scale * (1-alu) * mlu * proportional_power(pe, net, mode))."""
    return _path_cost_from_prop(pe, proportional_power(pe, net, mode), scale)


def network_cost(net):
    """(1-alu_net) * mlu_net * (p_total/p_max); 0.0 when p_max <= 0."""
    if net.p_max <= 0:
        return 0.0
    return (1.0 - net.alu_net) * net.mlu_net * (net.p_total / net.p_max)


def reward_from_cost(c):
    """reward = 1 - clamp01(cost)."""
    return 1.0 - clamp01(c)


def reward_terms(pe, net, w, mode, scale):
    """All intermediate terms for JSONL logging: prop, path_cost, network_cost, cost, reward."""
    prop = proportional_power(pe, net, mode)
    pc = _path_cost_from_prop(pe, prop, scale)
    nc = network_cost(net)
    cost = w * pc + (1.0 - w) * nc
    return {"prop": prop, "path_cost": pc, "network_cost": nc,
            "cost": cost, "reward": reward_from_cost(cost)}


def hybrid_rewards(snapshot, path_keys, w, mode, scale):
    """{path_key: reward} over path_keys (or every snapshot path when None);
    network_cost computed once.

    w = path-term weight, mode = proportional_power mode, scale = path-term scale.
    Keys missing from snapshot.paths or with n_hops == 0 are skipped.
    Returns a NEW plain dict.
    """
    net = snapshot.network
    paths = snapshot.paths
    keys = path_keys if path_keys is not None else list(paths.keys())
    nc = network_cost(net)
    out = {}
    for key in keys:
        pe = paths.get(key)
        if pe is None or not pe.n_hops:
            continue
        pc = path_cost(pe, net, mode, scale)
        out[key] = reward_from_cost(w * pc + (1.0 - w) * nc)
    return out
