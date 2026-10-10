# How it works

Parameter names refer to [`setting.py`](../setting.py); the main ones are listed in
[configuration-and-outputs.md](configuration-and-outputs.md).

## Modules

| Module | Role |
|---|---|
| `topology_manager.py` | Discovers switches, links and hosts; keeps `k` candidate paths per lower-leaf pair |
| `network_monitor.py` | Polls port statistics; publishes link utilisation and the per-path / network energy snapshot |
| `network_delay.py` | Measures LLDP link delay and feeds the per-path delay rank |
| `forwarding.py` | Packet-in handling, path choice, flow and meter installation, the agent cycle, the flow ledger |
| `packet_in_worker_pool.py` | Worker greenlets for packet-ins, one serial worker for MPTCP, the ledger writer |
| `bw_limit.py`, `meter_manager.py` | Per-path limit formula; per-switch meter pool |
| `power_model.py`, `energy_reward.py` | Link-power model and energy aggregates; the reward |
| `RL/` | PPO agent (`agents/ppo_agent.py`), network, replay buffer, hyperparameters (`config.py`) |
| `arp_handler.py` | ARP proxy: answers ARP for known hosts, delivers or floods the rest |
| `rest_api_v2.py` | REST API and dashboard on port 8080 |

## Pipeline

1. **Topology.** `topology_manager.py` discovers switches and links (LLDP), learns host
   locations, and keeps `k = 20` candidate paths per lower-leaf pair (`"<src>-<dst>-pathN"`
   keys). The paths are loaded from `data/all_paths/` at start-up and recomputed with
   Yen's algorithm only if those files are missing.
2. **Monitoring.** `network_monitor.py` polls port statistics every `LINK_MONITOR_INTERVAL`
   (10 s), turns them into bounded link utilisation, and publishes an energy snapshot:
   per-path power, average and maximum link utilisation (`ALU_p`, `MLU_p`), and the
   network totals (`P_total`, `ALU_net`, `MLU_net`). `network_delay.py` sends its raw LLDP
   delay table (switch-to-controller latency included) to the monitor every `DELAY_INTERVAL`
   (20 s); the monitor sums it per path and maps it to a delay rank (0, 0.3, 0.6 or 1 at 30,
   60 and 100 ms).
3. **Reward and path probabilities.** Every `AGENT_UPDATE_INTERVAL` (10 s) `forwarding.py`
   computes a reward for every path on which a forward flow (plain-TCP forward entry or
   MPTCP subflow) was installed or removed within `AGENT1_ACTIVITY_WINDOW_S` (20 s); if no
   path qualifies for `AGENT1_EMPTY_REWARD_FALLBACK_CYCLES` (3) consecutive cycles, every
   path is rewarded once and the count restarts. It then updates the PPO agent (`RL/`) and
   turns the agent's output into a probability per path.
4. **Bandwidth limits.** From the probabilities, the per-path `MLU` and the number of
   active flows, `bw_limit.py` computes one Mbps limit per path.
5. **Flow installation.** On a packet-in the worker pool (`packet_in_worker_pool.py`)
   picks a path uniformly at random among the agent's top `TOP_N_PATHS` (12) for the pair,
   installs the flow entries along it and attaches a meter (`meter_manager.py`) at the
   ingress switch of each direction: both directions of a plain-TCP flow, the one direction
   of an MPTCP subflow. UDP and other IPv4 traffic is routed without a meter. MPTCP subflows
   are recognised by the MP_CAPABLE / MP_JOIN options and spread over different paths. When
   a metered entry idles out, the switch's FlowRemoved message releases the meter and the
   per-flow record is written.

## Formulas

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
destination pair (`BW_PROB_NORMALIZATION = 'max'`). `BW_BOTTLENECK_CAP_Mbps` is
`LINK_MAX_CAPACITY` in Mbps (400); `PROB_EXP` and `RESID_EXP` are 1.0; the smoothing weight
is `BW_SMOOTH_ALPHA` (0.5) and the quantum `BW_QUANTUM_Mbps` (0.5).
