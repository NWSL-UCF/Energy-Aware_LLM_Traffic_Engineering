"""Controller configuration.

The controller's parameters live in this file, grouped by subsystem (the PPO
hyperparameters are in RL/config.py). Values are read once at start-up; restart the
controller after changing them.

Sections
  1. Topology                 - fabric size and names, path set
  2. Flow entries and cookies - priorities, timeouts, cookie spaces
  3. Monitoring and logs      - polling intervals, output files
  4. Energy model and reward  - link-power model, Agent1 reward
  5. Bandwidth limiting       - per-flow limit formula and meters
  6. Packet-in workers        - worker pool, MPTCP tracking
"""

# =============================================================================
# 1. Topology
# =============================================================================
# Two-level spine-leaf fabric: spines, upper leaves (ULeaf) and lower leaves (LLeaf) with
# the hosts attached to the lower leaves. Every lower leaf has one uplink, on port
# ULEAF_DOWNLINK_PORT of its upper leaf. The names must match the node names used in
# data/topology_stats/topology_stats_spine-leaf.json and the data/all_paths/ files
# describe paths between the lower leaves.
SPINE_DPIDS = range(1, 21)
ULEAF_DPIDS = range(101, 119)
LLEAF_DPIDS = range(201, 219)
ULEAF_DOWNLINK_PORT = 21
DPID_TO_NAME = {}
DPID_TO_NAME.update({d: f"Spine{d}" for d in SPINE_DPIDS})
DPID_TO_NAME.update({d: f"ULeaf{d - 100}" for d in ULEAF_DPIDS})
DPID_TO_NAME.update({d: f"LLeaf{d - 200}" for d in LLEAF_DPIDS})

# Lower leaves have a single uplink, so their ports carry no path choice: leave them
# (and the upper-leaf downlinks towards them) out of utilisation monitoring.
EXCLUDE_LLEAF_MONITORING = True

# Candidate paths per lower-leaf pair (Yen's k-shortest paths). Used when paths are
# computed; with USE_ALL_PATHS_START the shipped path files decide.
k = 20
# Load the pre-computed paths from data/all_paths/ at start-up instead of recomputing them.
USE_ALL_PATHS_START = True
# Seconds between periodic topology refreshes.
scheduler_interval = 300
# Nominal link attributes reported by the topology API (not measured).
default_bandwidth = 100   # Mbps
default_delay = 1         # ms

ETH_TYPE_ARP = 0x0806

# =============================================================================
# 2. Flow entries and cookies
# =============================================================================
# OpenFlow priorities. Dynamic flows beat the controller punts (priority 1).
PRIORITY_MPTCP = 950      # MPTCP subflow entries
PRIORITY_TCP = 850        # TCP, UDP and other IPv4 entries, and the pre-install for MPTCP packets
PRIORITY_ARP = 1          # punt ARP to the controller
PRIORITY_IPV4 = 1         # punt unknown IPv4 to the controller

# Timeouts of dynamic flow entries (seconds). An entry expires after IDL_TIME without
# traffic; its FlowRemoved message releases the meter and writes the per-flow record.
IDL_TIME = 30
HARD_TIMEOUT = 0          # 0 = no hard timeout

# Path selection: a new flow is drawn uniformly from the TOP_N_PATHS most probable
# paths of its pair (MPTCP subflows avoid paths already used by the same connection).
TOP_N_PATHS = 12

# False starts a shortest-path controller: no agent cycles, every flow takes the first
# path of its pair, and while ENABLE_BW_LIMIT is on every flow is metered at
# BW_DEFAULT_Mbps. With True the path selection can still be switched between the agent
# and shortest path at runtime through POST /api/v1/agents/agent1/enable.
USE_AGENT1 = True

# Cookie spaces, told apart by bits 31..29. The cookie ties a switch's FlowRemoved
# message back to the meter and the flow-ledger entry of the flow.
#   0                        punts, UDP entries and the MPTCP pre-install
#   MPTCP_COOKIE_BASE | n    010 -> MPTCP subflow entries
#   TCP_COOKIE_BASE   | n    011 -> plain TCP entries (one cookie per direction)
COOKIE_ID = 0x0
MAX_COOKIE_ID = 0x1FFFFFFF        # 29-bit counter shared by both dynamic spaces
MPTCP_COOKIE_BASE = 0x40000000
TCP_COOKIE_BASE = 0x60000000
DYNAMIC_COOKIE_MASK = 0xC0000000  # bits 31+30: matches every dynamic-flow cookie (TCP and MPTCP)
DYNAMIC_COOKIE_BASE = 0x40000000  # value of those bits for a dynamic-flow cookie
COOKIE_TYPE_MASK = 0xE0000000     # bits 31..29: identifies the space


def cookie_kind(cookie):
    """Return 'mptcp', 'tcp' or 'none' for a flow cookie."""
    if cookie is None:
        return 'none'
    tag = cookie & COOKIE_TYPE_MASK
    if tag == MPTCP_COOKIE_BASE:
        return 'mptcp'
    if tag == TCP_COOKIE_BASE:
        return 'tcp'
    return 'none'


def cookie_is_dynamic(cookie):
    """True for cookies of the dynamic (TCP or MPTCP) flow spaces."""
    return cookie is not None and (cookie & DYNAMIC_COOKIE_MASK) == DYNAMIC_COOKIE_BASE


# =============================================================================
# 3. Monitoring and logs
# =============================================================================
LINK_MONITOR_INTERVAL = 10          # seconds between port-statistics polls
LINK_MAX_CAPACITY = 400_000_000     # link capacity in bit/s (400 Mbps)
UTILIZATION_MAX = 1.0               # utilisation is clamped to [0, UTILIZATION_MAX]
DELAY_INTERVAL = 20                 # seconds between link-delay measurements

# Output files (one per run, named with the start timestamp).
ENABLE_POWER_LOG = True             # data/power/power_<ts>.jsonl, one row per monitor cycle
ENABLE_ENERGY_JSONL = True          # data/energy/agent1_cycle_<ts>.jsonl, one row per agent cycle
FLOW_REMOVED_LOG_ENABLED = True     # data/flows/flow_removed_<ts>.jsonl, one row per removed flow
ENERGY_LOG_MAX_MB = 512             # agent1_cycle and flow_removed streams rotate above
                                    # this size (the power log does not rotate)

# Verbose logging (large logs; off for normal runs).
AGENT1_LOGGING = False              # per-cycle agent logs
TM_LOGGING = False                  # per-pair path-computation logs

# =============================================================================
# 4. Energy model and Agent1 reward
# =============================================================================
# Linear link power: P_link = utilisation * POWER_CONSUMPTION_FACTOR (power_model.py).
POWER_CONSUMPTION_FACTOR = 40

# Seconds between agent cycles (reward, policy update, new probabilities, new limits).
AGENT_UPDATE_INTERVAL = 10

# reward_p = 1 - clamp01( w * path_cost_p + (1 - w) * network_cost )
#   path_cost_p  = clamp01( scale * (1 - ALU_p) * MLU_p * prop_p )
#   network_cost = (1 - ALU_net) * MLU_net * P_total / P_max
AGENT1_REWARD_PATH_WEIGHT = 0.5           # w: 1.0 = per-path term only, 0.0 = network term only
AGENT1_PROP_POWER_MODE = 'relative'       # prop_p: 'relative' = path mean link power / fabric mean,
                                          #         'raw' = P_p / P_total
AGENT1_PATH_TERM_SCALE = 1.0              # multiplies the per-path cost term
AGENT1_REWARD_ALL_PATHS = False           # False = reward only paths that carried a flow recently
AGENT1_EMPTY_REWARD_FALLBACK_CYCLES = 3   # after this many cycles without any active path,
                                          # reward all paths once
AGENT1_ACTIVITY_WINDOW_S = 20             # a path counts as active if a forward flow (plain-TCP
                                          # forward entry or MPTCP subflow) was installed or
                                          # removed on it within this many seconds

# Model persistence (models/ppo_agent1_model_final.pt).
AGENT1_NEW_MODEL = True                   # False = load the saved model at start-up
AGENT1_SAVE_MODEL = True                  # save it every 5 minutes and on shutdown (when the
                                          # agent ran, i.e. USE_AGENT1 is True)

# =============================================================================
# 5. Bandwidth limiting
# =============================================================================
# Every plain-TCP flow (both directions) and every MPTCP subflow gets an OpenFlow meter
# at its ingress switch. Limits are computed once per path per agent cycle (bw_limit.py):
#   resid = clamp01((U_TARGET - MLU_p) / U_TARGET)
#   share = p_norm ** PROB_EXP * resid ** RESID_EXP
#   limit = clamp(BW_BOTTLENECK_CAP_Mbps * share / max(1, n_active), BW_MIN_Mbps, BW_MAX_Mbps)
# followed by exponential smoothing and quantisation.
ENABLE_BW_LIMIT = True
BW_LIMIT_DRY_RUN = False              # True: compute and export limits, install no meters
BW_MIN_Mbps = 60                      # per-flow floor
BW_MAX_Mbps = 150                     # per-flow ceiling
BW_DEFAULT_Mbps = BW_MAX_Mbps         # limit of paths without a computed limit (before the
                                      # first agent cycle, or always when USE_AGENT1 is False)
U_TARGET = 0.85                       # target path utilisation
PROB_EXP = 1.0                        # exponent on the normalised path probability
RESID_EXP = 1.0                       # exponent on the residual-capacity factor
BW_QUANTUM_Mbps = 0.5                 # quantisation step (fewer meter changes)
BW_SMOOTH_ALPHA = 0.5                 # weight of the new value in the smoothing (1.0 = none)
BW_PROB_NORMALIZATION = 'max'         # 'max': p / highest probability of the pair;
                                      # 'sum': p / sum of the pair's probabilities
BW_BOTTLENECK_CAP_Mbps = LINK_MAX_CAPACITY / 1_000_000
BW_N_ACTIVE_INCLUDE_REVERSE = False   # count only forward-direction flows per path
BW_LIMIT_REVERSE_DIRECTION = True     # also meter the reverse entry of plain-TCP flows

# Meters form a re-use pool per ingress switch and are never deleted per flow; with
# CLEAN_METERS_ON_JOIN they are deleted when a switch joins and, if also
# CLEAN_METERS_ON_SHUTDOWN, when the controller stops.
MAX_METER_ID = 60000                  # highest meter id used per switch
METER_BURST_FRACTION = 0.10           # burst size as a fraction of the rate
CLEAN_METERS_ON_JOIN = True           # delete all meters of a switch when it connects
CLEAN_METERS_ON_SHUTDOWN = False      # delete all meters on Ctrl+C (slower exit)
METER_SHUTDOWN_DEADLINE_S = 1.0       # time budget for those deletes
METER_ERROR_BACKOFF_S = 60            # after a meter error, install unmetered on that
                                      # switch for this many seconds

# =============================================================================
# 6. Packet-in workers and MPTCP tracking
# =============================================================================
WORKER_POOL_SIZE = 16                 # green threads processing packet-ins
PACKET_QUEUE_MAXSIZE = 16384          # packet-in queue depth (further packets are dropped)
AGENT_QUEUE_MAXSIZE = 8192            # flow-ledger report queue depth
ENABLE_INSTALL_DEDUP = True           # a packet-in for a 5-tuple installed less than
INSTALL_DEDUP_WINDOW_S = 10           # this many seconds ago is forwarded, not re-installed
PATH_CACHE_TTL = 15                   # seconds a cached top-N path set per switch pair stays
                                      # valid (also cleared every agent cycle)

MPTCP_QUEUE_MAXSIZE = 4096            # queue of the serialised MPTCP worker
MPTCP_CLEANUP_INTERVAL = 30           # seconds between MPTCP state clean-ups
MPTCP_FLOW_TTL = 300                  # seconds after which an idle connection is dropped
MPTCP_FLOWS_MAX = 8192                # tracked connections (least recently used is evicted)
ENABLE_MPTCP_JOIN_CORRELATION = True  # match MP_JOINs towards the initiator to their
                                      # connection through the client endpoint
