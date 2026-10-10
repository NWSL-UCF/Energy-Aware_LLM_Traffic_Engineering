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


