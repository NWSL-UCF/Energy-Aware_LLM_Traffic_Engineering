# Configuration and outputs

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
| `LINK_MAX_CAPACITY` | 400_000_000 | Link capacity in bit/s (400 Mbps) used for utilisation; also the cap `BW_BOTTLENECK_CAP_Mbps` of the limit formula |

## Output files

Every run writes new files named with the time each app started (`MM-DD_HH-MM-SS`, so the
files of one run can differ by a second; the controller log uses `YYYYMMDD_HHMMSS`).

| File | Content |
|---|---|
| `data/controller_log/log-<ts>.txt` | Console log |
| `data/power/power_<ts>.jsonl` | One row per monitor cycle: `cycle, ts, p_total, p_max, p_norm, alu_net, mlu_net, n_links, dt_s, energy_cum` (`energy_cum` is the running integral of `p_total` over time) |
| `data/energy/agent1_cycle_<ts>.jsonl` | One row per agent cycle: `cycle` (monitor snapshot), `ts`, `agent1_cycle`, `network` (`p_total, alu_net, mlu_net, p_max, n_links, ts`), `reward` (n, mean, min, max and the reward settings), `bw_limits` (limit summary), `ledger` (flow counts), `meters` (active meter count), `terms` (per-path reward terms; empty when more than 500 paths were rewarded). Rotated above `ENERGY_LOG_MAX_MB` |
| `data/flows/flow_removed_<ts>.jsonl` | One row per removed metered entry, from its ingress switch (each direction of a plain-TCP flow, each MPTCP subflow; UDP is not recorded): `ts, dpid, cookie, kind` (tcp/mptcp), `direction, path_key, meter_id, byte_count, packet_count, duration_sec, reason, priority, bw_limit_mbps, installed_ts`. Rotated above `ENERGY_LOG_MAX_MB` |
| `RL/training_metrics/training_metrics_<ts>.csv` | One row per optimizer step (six per PPO update): step, total / policy / value loss, policy entropy, mean batch reward, clip fraction |
| `models/ppo_agent1_model_final.pt` | Agent checkpoint, written at most every 5 minutes (after an agent update) and on shutdown, when `AGENT1_SAVE_MODEL` is on and the agent ran (`USE_AGENT1 = True`); loaded at start when `AGENT1_NEW_MODEL = False` |
