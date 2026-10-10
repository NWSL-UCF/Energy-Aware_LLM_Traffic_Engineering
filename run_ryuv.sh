#!/bin/bash
# Start the controller. Run from the repository root (data/ and models/ paths are relative).
#
# --observe-links turns on LLDP link discovery in ryu.topology.switches (started before the
# apps as the delay detector's context); without it no links, paths or link ports are found.
# Apps are loaded in this order because later apps look up earlier ones at start-up.
# Ports: OpenFlow 6653 and REST/dashboard 8080 are ryu's defaults; change them with
#   --ofp-tcp-listen-port <port>   and   --wsapi-port <port>
set -u
mkdir -p data/controller_log
LOGFILE="data/controller_log/log-$(date +"%Y%m%d_%H%M%S").txt"
echo "Starting the controller, logging to $LOGFILE"

# tee runs in a subshell that ignores SIGINT so the shutdown lines written after Ctrl+C
# still reach the log file; it exits when ryu closes the pipe.
ryu-manager --observe-links \
    topology_manager.py network_monitor.py forwarding.py arp_handler.py network_delay.py rest_api_v2.py \
    2>&1 | ( trap '' INT; tee "$LOGFILE" )
