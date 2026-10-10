"""Pure per-path bandwidth-limit computation (no ryu imports).

Given the Agent1 path probabilities, the per-path max link utilisation (MLU)
and the number of active flows per path, this module produces a per-path
Mbps limit that forwarding.py turns into an OpenFlow meter.  Everything here
is side-effect free and returns a new dict; callers rebind it atomically so
concurrent readers never see a partially updated mapping.

Formula:

    resid  = clamp01((U_TARGET - clamp01(mlu)) / U_TARGET)
    share  = p_norm ** PROB_EXP * resid ** RESID_EXP
    raw    = cap_mbps * share / max(1, n_active)
    lim    = clamp(raw, bw_min, bw_max)
    lim    = alpha * lim + (1 - alpha) * prev          (if prev is not None)
    lim    = round(lim / quantum) * quantum            (if quantum > 0)
    return clamp(lim, bw_min, bw_max)
"""
from collections import namedtuple

import setting
from power_model import clamp01

BwConfig = namedtuple(
    "BwConfig",
    "bw_min bw_max u_target prob_exp resid_exp quantum alpha cap_mbps normalization")


def _clamp(x, lo, hi):
    """Return min(hi, max(lo, x))."""
    return min(hi, max(lo, x))


def config_from_setting():
    """Build a BwConfig from the BW_* / U_TARGET / *_EXP knobs of setting.

    Fields: BW_MIN_Mbps, BW_MAX_Mbps, U_TARGET, PROB_EXP, RESID_EXP,
    BW_QUANTUM_Mbps, BW_SMOOTH_ALPHA, BW_BOTTLENECK_CAP_Mbps, BW_PROB_NORMALIZATION.
    """
    s = setting
    return BwConfig(
        bw_min=float(s.BW_MIN_Mbps),
        bw_max=float(s.BW_MAX_Mbps),
        u_target=float(s.U_TARGET),
        prob_exp=float(s.PROB_EXP),
        resid_exp=float(s.RESID_EXP),
        quantum=float(s.BW_QUANTUM_Mbps),
        alpha=float(s.BW_SMOOTH_ALPHA),
        cap_mbps=float(s.BW_BOTTLENECK_CAP_Mbps),
        normalization=str(s.BW_PROB_NORMALIZATION),
    )


def pair_of(path_key):
    """Return the src-dst pair of a path key: path_key.rsplit("-", 1)[0].

    "201-202-path7" -> "201-202".
    """
    return str(path_key).rsplit("-", 1)[0]


def normalize_probabilities(probs, mode="max"):
    """Normalise path probabilities per src-dst pair.

    denom(pair) = max(p) for mode "max", sum(p) for mode "sum";
    p_norm = clamp01(p / denom), 0.0 when denom <= 0.
    Unknown mode -> ValueError.  Returns a new dict {path_key: p_norm}.
    """
    if mode not in ("max", "sum"):
        raise ValueError("unknown normalization mode: %r" % (mode,))
    denom = {}
    for key, p in probs.items():
        pair = pair_of(key)
        p = float(p)
        if mode == "max":
            cur = denom.get(pair)
            if cur is None or p > cur:
                denom[pair] = p
        else:
            denom[pair] = denom.get(pair, 0.0) + p
    out = {}
    for key, p in probs.items():
        d = denom.get(pair_of(key), 0.0)
        out[key] = clamp01(float(p) / d) if d > 0 else 0.0
    return out


def compute_bw_limit(p_norm, mlu, n_active, prev, cfg):
    """Per-path Mbps limit.

    resid = clamp01((u_target - clamp01(mlu)) / u_target);
    share = p_norm**prob_exp * resid**resid_exp;
    raw = cap_mbps * share / max(1, int(n_active or 0));
    lim = clamp(raw, bw_min, bw_max); EWMA with prev (alpha on the new value);
    quantize to cfg.quantum (Python round); re-clamp.  Returns float.
    """
    u_target = float(cfg.u_target)
    if u_target > 0:
        resid = clamp01((u_target - clamp01(mlu)) / u_target)
    else:
        resid = 0.0
    share = (clamp01(p_norm) ** cfg.prob_exp) * (resid ** cfg.resid_exp)
    raw = cfg.cap_mbps * share / max(1, int(n_active or 0))
    lim = _clamp(raw, cfg.bw_min, cfg.bw_max)
    if prev is not None:
        lim = cfg.alpha * lim + (1.0 - cfg.alpha) * float(prev)
    if cfg.quantum > 0:
        lim = round(lim / cfg.quantum) * cfg.quantum
    return float(_clamp(lim, cfg.bw_min, cfg.bw_max))


def compute_all_limits(probs, mlus, n_active_by_path, prev_limits, cfg):
    """Compute limits for every path key in probs.

    p_norm = normalize_probabilities(probs, cfg.normalization)[key];
    limit[key] = compute_bw_limit(p_norm, mlus.get(key, 0.0),
                                  n_active_by_path.get(key, 0),
                                  prev_limits.get(key), cfg).
    Returns a NEW dict {path_key: limit}; pass it back as prev_limits next cycle.
    """
    mlus = mlus or {}
    n_active_by_path = n_active_by_path or {}
    prev_limits = prev_limits or {}
    normed = normalize_probabilities(probs, cfg.normalization)
    limits = {}
    for key, p_norm in normed.items():
        limits[key] = compute_bw_limit(p_norm,
                                       mlus.get(key, 0.0),
                                       n_active_by_path.get(key, 0),
                                       prev_limits.get(key),
                                       cfg)
    return limits


def summarize(limits, cfg):
    """Summary of a {path_key: limit} dict for logging.

    {"n", "min", "median", "max", "mean", "n_at_min", "n_at_max"}; all zeros when empty.
    median = middle element (odd n) or mean of the two middle elements (even n).
    """
    vals = sorted(float(v) for v in limits.values()) if limits else []
    n = len(vals)
    if n == 0:
        return {"n": 0, "min": 0.0, "median": 0.0, "max": 0.0, "mean": 0.0,
                "n_at_min": 0, "n_at_max": 0}
    if n % 2:
        median = vals[n // 2]
    else:
        median = (vals[n // 2 - 1] + vals[n // 2]) / 2.0
    return {
        "n": n,
        "min": vals[0],
        "median": median,
        "max": vals[-1],
        "mean": sum(vals) / n,
        "n_at_min": sum(1 for v in vals if v <= float(cfg.bw_min)),
        "n_at_max": sum(1 for v in vals if v >= float(cfg.bw_max)),
    }
