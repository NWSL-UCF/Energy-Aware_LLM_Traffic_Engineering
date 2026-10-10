# Energy-aware SDN controller with RL path selection

A [Ryu](https://ryu-sdn.org/) / OpenFlow 1.3 controller for a spine-leaf data-centre
fabric. A PPO reinforcement-learning agent chooses the path of every TCP flow and every
MPTCP subflow. Every TCP flow and MPTCP subflow is rate-limited at its ingress switch by
an OpenFlow meter whose rate is the path's current limit when the flow is installed
(UDP is routed but not metered); the per-path limits are recomputed every agent cycle
from a link-power model and apply to new flows. The goal is
to concentrate traffic on fewer links so that the fabric's modelled power consumption
drops, while keeping each path below a target utilisation.

The controller runs on one machine; the switches (Open vSwitch) run elsewhere and connect
to it over OpenFlow. A web dashboard and a REST API are served on port 8080.

## How it works

1. **Topology.** `topology_manager.py` discovers switches and links (LLDP), learns host
   locations, and keeps `k = 20` candidate paths per lower-leaf pair (`"<src>-<dst>-pathN"`
   keys). The paths are loaded from `data/all_paths/` at start-up and recomputed with
   Yen's algorithm only if those files are missing.
2. **Monitoring.** `network_monitor.py` polls port statistics every `LINK_MONITOR_INTERVAL`
   (10 s), turns them into bounded link utilisation, and publishes an energy snapshot:
   per-path power, average and maximum link utilisation (`ALU_p`, `MLU_p`), and the
   network totals (`P_total`, `ALU_net`, `MLU_net`). `network_delay.py` measures the LLDP
   delay of every link and sends the table to the monitor, which sums it per path and maps
   it to a delay rank (0, 0.3, 0.6 or 1 at 30, 60 and 100 ms).
3. **Reward and path probabilities.** Every `AGENT_UPDATE_INTERVAL` (10 s) `forwarding.py`
   computes a reward for every path on which a forward flow (plain-TCP forward entry or
   MPTCP subflow) was installed or removed within `AGENT1_ACTIVITY_WINDOW_S` (all paths
   after `AGENT1_EMPTY_REWARD_FALLBACK_CYCLES` idle
   cycles), updates the PPO agent (`RL/`), and turns the agent's output into a probability
   per path.
4. **Bandwidth limits.** From the probabilities, the per-path `MLU` and the number of
   active flows, `bw_limit.py` computes one Mbps limit per path.
5. **Flow installation.** On a packet-in the worker pool (`packet_in_worker_pool.py`)
   picks a path among the agent's top `TOP_N_PATHS`, installs the flow entries along it
   and attaches a meter (`meter_manager.py`) at the ingress switch. MPTCP subflows are
   recognised by the MP_CAPABLE / MP_JOIN options and spread over different paths. When a
   flow idles out, the switch's FlowRemoved message releases the meter and the per-flow
   record is written.

### Formulas

Link power (`power_model.py`), linear in utilisation; `POWER_CONSUMPTION_FACTOR` is the
power of a fully used link in watts and only scales the reported power, not the reward
or the limits:

```
P_link  = util * POWER_CONSUMPTION_FACTOR
P_p     = sum of P_link over the path's monitored links
P_total = sum over all monitored links,  P_max = N_links * link_power(1.0)
```

Reward of path p (`energy_reward.py`):

```
prop_p       = (P_p / n_hops_p) / (P_total / N_links)            # 'relative' mode
path_cost_p  = clamp01( scale * (1 - ALU_p) * MLU_p * prop_p )
network_cost = (1 - ALU_net) * MLU_net * (P_total / P_max)
reward_p     = 1 - clamp01( w * path_cost_p + (1 - w) * network_cost )
```

Bandwidth limit of path p in Mbps (`bw_limit.py`):

```
resid = clamp01( (U_TARGET - MLU_p) / U_TARGET )
share = p_norm ** PROB_EXP * resid ** RESID_EXP
raw   = BW_BOTTLENECK_CAP_Mbps * share / max(1, n_active_p)
limit = clamp(raw, BW_MIN_Mbps, BW_MAX_Mbps), then EWMA-smoothed and quantised
```

`p_norm` is the path's probability divided by the highest probability of its source–
destination pair (`BW_PROB_NORMALIZATION = 'max'`).

## Requirements

- Ubuntu 20.04 (or similar) with **Python 3.8**. Ryu 4.34 and eventlet 0.30.2 do not run on
  newer interpreters.
- The Python packages in `requirements.txt` (ryu, eventlet, networkx, numpy, torch). The
  agent runs on the CPU, so the CPU build of torch is enough.
- Open vSwitch 2.10 or newer on the switches: meters on the kernel datapath need it.
- Hosts with an MPTCP-capable kernel only if you want MPTCP traffic. Plain TCP works as is.
- The dashboard loads Plotly, jQuery and D3 from public CDNs, so the browser that opens it
  needs internet access. The controller itself does not.

## Install

```bash
python3.8 -m venv venv
source venv/bin/activate
pip install torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
```

## Topology and input files

The controller is written for a two-level spine-leaf fabric. The datapath ids, the
switch names and the port that connects each upper leaf to its lower leaf are defined in
section 1 of `setting.py` (`SPINE_DPIDS`, `ULEAF_DPIDS`, `LLEAF_DPIDS`, `ULEAF_DOWNLINK_PORT`,
`DPID_TO_NAME`); the shipped values are:

| Role | DPIDs | Name used in statistics |
|---|---|---|
| Spine | 1 – 20 | `Spine1` … `Spine20` |
| Upper leaf (ULeaf) | 101 – 118 | `ULeaf1` … `ULeaf18` |
| Lower leaf (LLeaf) | 201 – 218 | `LLeaf1` … `LLeaf18` |

Hosts attach to the lower leaves. Each lower leaf has a single uplink, so paths are chosen
between lower leaves across the upper leaves and spines. Hosts are dual-homed on
`10.0.A.x` / `10.0.B.x`; the controller identifies a host by the last octet of its
address when it correlates MPTCP subflows, so keep that addressing if you use MPTCP.

Four input files under `data/` describe the fabric. The testbed itself (Mininet or OVS
virtual machines) and the scripts that generated these files are not part of this
repository; to run on a different fabric, produce the same files for it.

| File | Content |
|---|---|
| `data/topology_stats/topology_stats_spine-leaf.json` | `ports`: one entry per switch port, keyed `"<Name>-port<N>"` where `N` is the OpenFlow port number, with `{"connected_to", "dst-dpid": "<neighbour name>", "bandwidth", "delay"}`; this is the part the controller reads (it builds the monitored link list from it). `links`: one entry per link, keyed `"<node1>-<node2>"`, kept for reference. |
| `data/access_table/access_table_edge_spine-leaf.json` | `"<dpid>:<port>": [ip, mac]` for every host port. Loaded at start; a host learned at runtime that is not in it is added and the file is rewritten. |
| `data/all_paths/all_paths_spine-leaf.json` | The `k` candidate paths per lower-leaf pair, keyed `"<src>-<dst>-pathN"`; each hop is `[dpid, {src_port, dst_port, bandwidth, delay}]`. |
| `data/all_paths/all_paths_dpid_spine-leaf.json` | The same paths as plain DPID lists. |

If the path files are missing, both are recomputed once all switches of the fabric have
connected and saved as `all_paths_<epoch>.json` and `all_paths_dpid_<epoch>.json`; the
newest such pair is used at the next start.

## Run

From the repository root:

```bash
bash run_ryuv.sh
```

The script starts `ryu-manager --observe-links` with the six applications and tees the
console to `data/controller_log/log-<timestamp>.txt`. Point the switches at
`tcp:<controller-ip>:6653`. The dashboard is at `http://<controller-ip>:8080/dashboard`.
To change the ports, add `--ofp-tcp-listen-port` or `--wsapi-port` to the `ryu-manager`
line in `run_ryuv.sh`.

Stop with Ctrl+C. The controller saves the agent model and flushes its log files before
exiting.

For a first run on a new testbed set `BW_LIMIT_DRY_RUN = True`: limits are computed and
exported but no meters are installed, so you can check the numbers before they act on
traffic.

### Dashboard pages

| Page | Shows |
|---|---|
| `/dashboard` | System overview, network energy, link utilisation, per-path state |
| `/topology-view` | Discovered switches, hosts and links (switches appear about 30 s after the last one joins) |
| `/flow-monitoring` | Tracked flows (from the controller's flow ledger), current bandwidth limits, per-path rewards |
| `/agent-control` | Agent status, RL on/off switch, path probabilities |

### REST API

| Endpoint | Returns |
|---|---|
| `GET /api/v1/system/overview` | Switch / link / host counts, agent state, the latest energy row |
| `GET /api/v1/topology` | Topology summary |
| `GET /api/v1/topology/details` | Nodes and links as drawn by the topology page |
| `GET /api/v1/topology/hosts` | Known hosts |
| `GET /api/v1/topology/paths/{src}/{dst}` | Candidate paths between two lower leaves |
| `GET /api/v1/stats/utilization` | Link and port utilisation |
| `GET /api/v1/stats/paths`, `/api/v1/stats/paths/{path_id}` | Per-path state `(P_norm, ALU, MLU, delay_rank)` |
| `GET /api/v1/delay/stats`, `/api/v1/delay/link/{src}/{dst}` | Measured link delays |
| `GET /api/v1/flows/tracked` | Flow ledger counts (TCP forward/reverse, MPTCP) |
| `GET /api/v1/flows/rewards` | Latest reward per path |
| `GET /api/v1/mptcp/flows` | Tracked MPTCP connections and subflows |
| `GET /api/v1/agents/status` | Agent state and device |
| `POST /api/v1/agents/agent1/enable` | Body `{"enabled": true/false}`: switch path selection between the RL agent and shortest path at runtime (limits and learning continue; needs `USE_AGENT1 = True` at start) |
| `GET /api/v1/agents/agent1/probabilities` | Current path probabilities |
| `GET /api/v1/energy/network` | The latest network power row |
| `GET /api/v1/energy/paths?prefix=201-202&limit=` | Per-path energy state |
| `GET /api/v1/energy/rewards` | Rewards and their terms for the last cycle |
| `GET /api/v1/energy/bw-limits?prefix=` | Current per-path bandwidth limits |
| `GET /api/v1/energy/meters` | Meter pool state, OpenFlow error counters, worker-pool counters |
| `GET /api/v1/energy/flows` | Ledger counts and paths with recent activity |

The API has no authentication; keep port 8080 on a trusted management network.

## Configuration

The controller's parameters live in `setting.py`, grouped by subsystem and commented
(the PPO hyperparameters are in `RL/config.py`). The most relevant ones:

| Parameter | Default | Meaning |
|---|---|---|
| `IDL_TIME` | 30 | Idle timeout (s) of installed flow entries |
| `TOP_N_PATHS` | 12 | Size of the top-probability path set a new flow is drawn from |
| `AGENT_UPDATE_INTERVAL` | 10 | Seconds between agent cycles (reward, update, new probabilities, new limits) |
| `AGENT1_REWARD_PATH_WEIGHT` | 0.5 | `w`: weight of the per-path cost against the network cost |
| `POWER_CONSUMPTION_FACTOR` | 40 | Power (W) of a fully used link in the linear model |
| `ENABLE_BW_LIMIT` | True | Master switch for per-flow meters |
| `BW_LIMIT_DRY_RUN` | False | Compute and export limits but install no meters |
| `BW_MIN_Mbps`, `BW_MAX_Mbps` | 60, 150 | Per-flow limit range |
| `U_TARGET` | 0.85 | Path utilisation the limits aim to stay below |
| `BW_PROB_NORMALIZATION` | `'max'` | How a path's probability is normalised within its pair |
| `USE_AGENT1` | True | False: shortest path for every flow and no agent cycles; flows are still metered at `BW_DEFAULT_Mbps` while `ENABLE_BW_LIMIT` is on |
| `LINK_MAX_CAPACITY` | 400 Mbps | Link capacity used for utilisation |

## Output files

Every run writes new files named with its start time (`MM-DD_HH-MM-SS`; the controller log
uses `YYYYMMDD_HHMMSS`).

| File | Content |
|---|---|
| `data/controller_log/log-<ts>.txt` | Console log |
| `data/power/power_<ts>.jsonl` | One row per monitor cycle: `cycle, ts, p_total, p_max, p_norm, alu_net, mlu_net, n_links, dt_s, energy_cum` (`energy_cum` is the running integral of `p_total` over time) |
| `data/energy/agent1_cycle_<ts>.jsonl` | One row per agent cycle: `cycle, ts, agent1_cycle`, the network energy row, reward statistics, the per-path reward terms (only when 500 or fewer paths were rewarded), the bandwidth-limit summary, ledger counts, meter count |
| `data/flows/flow_removed_<ts>.jsonl` | One row per removed flow: cookie, kind (tcp/mptcp), direction, path key, meter id, bytes, packets, duration, removal reason, limit in Mbps |
| `RL/training_metrics/training_metrics_<ts>.csv` | One row per optimizer step (six per PPO update): step, total / policy / value loss, policy entropy, mean batch reward, clip fraction |
| `models/ppo_agent1_model_final.pt` | Agent checkpoint, written every 5 minutes and on shutdown when `AGENT1_SAVE_MODEL` is on and the agent ran (`USE_AGENT1 = True`); loaded at start when `AGENT1_NEW_MODEL = False` |

## License

No license has been chosen yet; add a `LICENSE` file before publishing.
