# Energy-Aware Traffic Engineering

**Energy-Aware Traffic Engineering** is a [Ryu](https://ryu-sdn.org/) / OpenFlow 1.3 SDN controller that uses a PPO agent to route every TCP flow and MPTCP subflow in a spine-leaf fabric. Each of these flows is rate-limited at its ingress switch by a per-path limit derived from the agent's path probabilities and the path's utilisation; the reward is designed to concentrate traffic on fewer links and lower the fabric's modelled power, while the limits aim to keep every path below a target utilisation.

## Quick Start

**1.** Install on Python 3.8 (Ryu 4.34 and eventlet 0.30.2 do not run on newer versions):

```bash
python3.8 -m venv venv && source venv/bin/activate
pip install torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu
pip install -r requirements.txt
```

**2.** Start the controller from the repository root and point the switches (Open vSwitch 2.10 or newer) at `tcp:<controller-ip>:6653`:

```bash
bash run_ryuv.sh
```

On a new testbed set `BW_LIMIT_DRY_RUN = True` in `setting.py` for the first run: limits are computed but no meters are installed.

**3.** Open the dashboard at `http://<controller-ip>:8080/dashboard`. Each run writes its log, power trace, per-cycle energy and per-flow records under `data/`, training metrics in `RL/training_metrics/` and the model in `models/`.

All parameters live in [`setting.py`](setting.py) (PPO hyperparameters in [`RL/config.py`](RL/config.py)). The controller expects a fabric of 20 spines, 18 upper leaves and 18 lower leaves, described by the input files under `data/`; the testbed itself (Mininet or OVS VMs) is not included.

## Documentation

| | |
|---|---|
| How it works (pipeline, modules, formulas) | [docs/how-it-works.md](docs/how-it-works.md) |
| Requirements, input files, running, dashboard and REST API | [docs/setup.md](docs/setup.md) |
| Parameters and output files | [docs/configuration-and-outputs.md](docs/configuration-and-outputs.md) |

## License

No license has been chosen yet; until one is added, all rights are reserved.

---

*Developed at the [Networking and Wireless Systems Lab (NWSL)](https://www.nwsl.ucf.edu/), University of Central Florida. All rights reserved.*
