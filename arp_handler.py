"""ARP proxy for the controller.

Learns IP-to-MAC bindings from the ARP packets punted by the switches, answers
ARP requests for already-known hosts straight from the controller, and otherwise
delivers the request to the destination host's access port, or floods it on
every port with no learned host when the destination's location is unknown.
Adapted from a Ryu ARP-proxy example.
"""
from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.ofproto import ofproto_v1_3
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.lib.packet import packet
from ryu.lib.packet import arp, ethernet
import logging


ETHERNET = ethernet.ethernet.__name__
ETHERNET_MULTICAST = "ff:ff:ff:ff:ff:ff"
ARP = arp.arp.__name__
LOG = logging.getLogger('ryu.app.arp_handler')


class ArpHandler(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ArpHandler, self).__init__(*args, **kwargs)
        self.discovery = lookup_service_brick('TopologyManagerV2')
        self.monitor = lookup_service_brick('NetworkMonitor')

        self.arp_table = {}   # {ip: mac} learned from ARP packets
        self.sw = {}          # {(dpid, src_mac, dst_ip): in_port} loop breaker

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """Handle punted ARP packets: proxy-reply, forward to the known host,
        or flood on every port with no learned host."""
        msg = ev.msg
        datapath = msg.datapath

        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        arp_pkt = pkt.get_protocol(arp.arp)

        eth = pkt.get_protocols(ethernet.ethernet)[0]
        src = eth.src
        header_list = dict((p.protocol_name, p) for p in pkt.protocols if type(p) != bytes)
        if isinstance(arp_pkt, arp.arp):
            self.arp_table[arp_pkt.src_ip] = src

            if self.arp_handler(header_list, datapath, in_port):
                # Replied or dropped; nothing more to do.
                return None
            else:
                arp_dst_ip = arp_pkt.dst_ip
                location = self.discovery.get_host_ip_location(arp_dst_ip)
                if location:
                    # The destination host's access port is known: deliver there.
                    dpid_dst, out_port = location
                    datapath = self.monitor.datapaths_table.get(dpid_dst)
                    if datapath is None:
                        LOG.debug("ARP: no datapath for dpid %s, dropping", dpid_dst)
                        return
                    out = self._build_packet_out(datapath, out_port, msg.data)
                    datapath.send_msg(out)
                else:
                    # Unknown host: flood on every switch port with no learned host
                    # (not in access_table); self.sw breaks the resulting loops.
                    for dpid in self.discovery.switch_all_ports_table:
                        datapath = self.monitor.datapaths_table.get(dpid)
                        if datapath is None:
                            LOG.debug("ARP: no datapath for dpid %s, skipping flood", dpid)
                            continue
                        for port in self.discovery.switch_all_ports_table[dpid]:
                            if (dpid, port) not in self.discovery.access_table.keys():
                                out = self._build_packet_out(datapath, port, msg.data)
                                datapath.send_msg(out)
                    return

    def arp_handler(self, header_list, datapath, in_port):
        """Return True when the packet was answered or dropped here, False when
        the caller must forward or flood it."""
        eth_dst = header_list[ETHERNET].dst
        eth_src = header_list[ETHERNET].src

        if eth_dst == ETHERNET_MULTICAST and ARP in header_list:
            arp_dst_ip = header_list[ARP].dst_ip
            if (datapath.id, eth_src, arp_dst_ip) in self.sw:
                # Same broadcast request seen again on this switch: drop it to break the loop.
                out = datapath.ofproto_parser.OFPPacketOut(
                    datapath=datapath,
                    buffer_id=datapath.ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=[], data=None
                )
                datapath.send_msg(out)
                return True
            else:
                self.sw[(datapath.id, eth_src, arp_dst_ip)] = in_port

        if ARP in header_list:
            opcode = header_list[ARP].opcode

            arp_src_ip = header_list[ARP].src_ip
            arp_dst_ip = header_list[ARP].dst_ip

            actions = []

            if opcode == arp.ARP_REQUEST:
                if arp_dst_ip in self.arp_table:
                    # Target MAC is known: answer from the controller.
                    actions.append(datapath.ofproto_parser.OFPActionOutput(in_port))

                    ARP_Reply = packet.Packet()
                    ARP_Reply.add_protocol(ethernet.ethernet(ethertype=header_list[ETHERNET].ethertype,
                                                             dst=eth_src,
                                                             src=self.arp_table[arp_dst_ip]))
                    ARP_Reply.add_protocol(arp.arp(opcode=arp.ARP_REPLY,
                                                   src_mac=self.arp_table[arp_dst_ip],
                                                   src_ip=arp_dst_ip,
                                                   dst_mac=eth_src,
                                                   dst_ip=arp_src_ip))

                    ARP_Reply.serialize()

                    out = datapath.ofproto_parser.OFPPacketOut(
                        datapath=datapath,
                        buffer_id=datapath.ofproto.OFP_NO_BUFFER,
                        in_port=datapath.ofproto.OFPP_CONTROLLER,
                        actions=actions,
                        data=ARP_Reply.data
                    )
                    datapath.send_msg(out)
                    return True
        return False

    def _build_packet_out(self, datapath, dst_port, data):
        """Build an unbuffered PacketOut (in_port = controller) that emits ``data``
        on ``dst_port``; returns None when there is no packet data."""
        if data is None:
            return None
        ofproto = datapath.ofproto
        actions = []
        if dst_port:
            actions.append(datapath.ofproto_parser.OFPActionOutput(dst_port))

        out = datapath.ofproto_parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER,
                                                   data=data, in_port=ofproto.OFPP_CONTROLLER,
                                                   actions=actions)

        return out
