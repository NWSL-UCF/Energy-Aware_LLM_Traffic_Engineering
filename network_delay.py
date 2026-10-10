"""Link delay measurement.

Combines the LLDP round trip measured by ryu's topology discovery with the
controller-to-switch echo latency to estimate the one-way delay of every
inter-switch link, and publishes the raw LLDP delay table to the network
monitor. Adapted from a Ryu delay-detector example.
"""
import logging
import time

from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub

from ryu.topology.switches import Switches, LLDPPacket

import setting
from events import EventDelayUpdate

LOG = logging.getLogger('ryu.app.network_delay')


class NetworkDelayDetector(app_manager.RyuApp):
    """Measure inter-switch link delay."""
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'switches': Switches}

    def __init__(self, *args, **kwargs):
        super(NetworkDelayDetector, self).__init__(*args, **kwargs)
        self.name = 'detector'

        self.network_monitor = lookup_service_brick('NetworkMonitor')
        self.switch_module = kwargs['switches']

        self.echo_delay_table = {}  # {dpid: controller<->switch echo latency}
        self.lldp_delay_table = {}  # {src_dpid: {dst_dpid: lldp delay}}
        self.echo_interval = 0.05

        self._delay_thread = hub.spawn(self.scheduler)

    def scheduler(self):
        while True:
            try:
                self._send_echo_request()
                self.send_delay_update()
            except Exception:
                LOG.exception("[delay] measurement cycle failed")
            hub.sleep(setting.DELAY_INTERVAL)

    def _send_echo_request(self):
        """Send one echo request per switch, carrying the send timestamp."""
        for datapath in list(self.network_monitor.datapaths_table.values()):
            parser = datapath.ofproto_parser
            data = time.time()
            echo_req = parser.OFPEchoRequest(datapath, b"%.12f" % data)
            datapath.send_msg(echo_req)
            hub.sleep(self.echo_interval)  # pace the requests so the replies are not lost

    @set_ev_cls(ofp_event.EventOFPEchoReply, MAIN_DISPATCHER)
    def _echo_reply_handler(self, ev):
        now_timestamp = time.time()
        try:
            sent_timestamp = float(ev.msg.data)
        except (TypeError, ValueError):
            return
        self.echo_delay_table[ev.msg.datapath.id] = now_timestamp - sent_timestamp

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Parse punted LLDP frames and record the per-direction LLDP delay."""
        try:
            recv_timestamp = time.time()
            msg = ev.msg
            dpid = msg.datapath.id
            src_dpid, src_port_no = LLDPPacket.lldp_parse(msg.data)

            # O(1) port lookup: an index {(dpid, port_no): Port} rebuilt only when the
            # switches app's port table changes size.
            ports = self.switch_module.ports
            idx = getattr(self, '_port_index', None)
            if idx is None or getattr(self, '_port_index_len', -1) != len(ports):
                idx = {(pt.dpid, pt.port_no): pt for pt in list(ports.keys())}
                self._port_index = idx
                self._port_index_len = len(ports)
            port = idx.get((src_dpid, src_port_no))
            if port is not None:
                send_timestamp = ports[port].timestamp
                if send_timestamp:
                    delay = recv_timestamp - send_timestamp
                    self.lldp_delay_table.setdefault(src_dpid, {})
                    self.lldp_delay_table[src_dpid][dpid] = delay
        except LLDPPacket.LLDPUnknownFormat:
            return

    def calculate_delay(self, src, dst):
        """Estimated one-way delay (s) of the src->dst link; raises KeyError
        until both LLDP directions and both echo latencies have been measured.

                        ┌------Ryu------┐
                        |               |
        src echo latency|               |dst echo latency
                        |               |
                    SwitchA------------SwitchB
                         --->fwd_delay--->
                         <---reply_delay<---
        """

        fwd_delay = self.lldp_delay_table[src][dst]
        reply_delay = self.lldp_delay_table[dst][src]
        ryu_ofps_src_delay = self.echo_delay_table[src]
        ryu_ofps_dst_delay = self.echo_delay_table[dst]

        delay = (fwd_delay + reply_delay - ryu_ofps_src_delay - ryu_ofps_dst_delay) / 2
        return max(delay, 0)

    def get_delay_stats(self, src, dst):
        delay = self.lldp_delay_table[src][dst]
        return delay

    def send_delay_update(self):
        """Send the LLDP delay table to the network monitor."""
        delay_event = EventDelayUpdate(self.lldp_delay_table)
        self.send_event('NetworkMonitor', delay_event)

    def close(self):
        """ryu calls close() on Ctrl+C; stop the echo scheduler greenlet."""
        try:
            self._cleanup()
        except Exception as e:
            self.logger.error(f"[delay] cleanup error: {e}")
        super(NetworkDelayDetector, self).close()

    def _cleanup(self):
        self.echo_delay_table.clear()
        self.lldp_delay_table.clear()

        hub.kill(self._delay_thread)

        self.logger.info("delay detector stop")
