# Setup and run

## Requirements

- Ubuntu 20.04 (or similar) with **Python 3.8**. Ryu 4.34 and eventlet 0.30.2 do not run on
  newer interpreters.
- The pinned packages in `requirements.txt` (ryu 4.34 with its tinyrpc / WebOb / Routes /
  dnspython pins, eventlet, networkx, numpy, torch). The agent runs on the CPU, so the CPU
  build of torch is enough.
- Open vSwitch 2.10 or newer on the switches: meters on the kernel datapath need it.
- Hosts with an MPTCP v1 (RFC 8684) kernel only if you want MPTCP traffic. Plain TCP works as is.
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
between lower leaves across the upper leaves and spines. Each host has two interfaces on
its lower leaf, `10.0.1.X` and `10.0.2.X`, with `X` unique across the fabric; the controller
identifies a host by that last octet when it correlates MPTCP subflows, so keep that
addressing if you use MPTCP.

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

Stop with Ctrl+C. The controller saves the agent model (when `AGENT1_SAVE_MODEL` and
`USE_AGENT1` are True) and gives the log writers a short drain before exiting.

For a first run on a new testbed set `BW_LIMIT_DRY_RUN = True` in `setting.py` (with
`ENABLE_BW_LIMIT = True`): limits are computed and exported but no meters are installed, so
you can check the numbers before they act on traffic.

### Dashboard pages

| Page | Shows |
|---|---|
| `/dashboard` | System overview, network energy, link utilisation, per-path state |
| `/topology-view` | Discovered switches, hosts and links (switches and links appear about 30 s after the last switch or link event) |
| `/flow-monitoring` | Tracked flows (from the controller's flow ledger), current bandwidth limits, per-path rewards |
| `/agent-control` | Agent status, RL on/off switch, path probabilities |

### REST API

| Endpoint | Returns |
|---|---|
| `GET /api/v1/system/overview` | Switch / link / host counts, agent state, the latest energy row |
| `GET /api/v1/topology` | Topology summary |
| `GET /api/v1/topology/details` | Nodes and links as drawn by the topology page |
| `GET /api/v1/topology/hosts` | Known hosts |
| `GET /api/v1/topology/paths/{src}/{dst}` | Candidate paths between two lower leaves, by DPID (e.g. `/201/202`) |
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
| `GET /api/v1/energy/bw-limits?prefix=&active_only=1` | Current per-path bandwidth limits (optionally only paths with live forward flows) |
| `GET /api/v1/energy/meters` | Meter pool state, OpenFlow error counters, worker-pool counters |
| `GET /api/v1/energy/flows` | Ledger counts and paths with recent activity |

The API has no authentication; keep port 8080 on a trusted management network.
