from typing import Dict
from collections import defaultdict
from ryu.base import app_manager
from ryu.ofproto import ofproto_v1_4
from ryu.controller import ofp_event
from ryu.lib.packet import packet, ethernet, arp
from ryu.ofproto import ether
from ryu.controller.controller import Datapath
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.topology import event as topo_event, switches as topo_sw
import networkx as nx

import utils


class Controller(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph = nx.Graph()
        self.id_counter = 1
        self.dpid_ports: Dict[int, Dict[int, str]] = defaultdict(dict)
        self.mac_to_dpid: Dict[str, int] = {}
        self.dps: Dict[int, Datapath] = {}

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    def add_mpls_pop(self, dp: Datapath):
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        instructions = [
            parser.OFPInstructionActions([parser.OFPActionPopMpls(ether.ETH_TYPE_MPLS)]),
            parser.OFPInstructionGotoTable(1)]
        mod = parser.OFPFlowMod(
            datapath=dp,
            table_id=0,
            priority=3,
            match=parser.OFPMatch(eth_type=ether.ETH_TYPE_MPLS),
            instructions=instructions
        )

        dp.send_msg(mod)

    def add_flow(self, datapath: Datapath, priority, match, actions, table_id=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                match=match, instructions=inst, table_id=table_id)
        datapath.send_msg(mod)

    def add_link_flows(self, dpid: int, port: topo_sw.Port):
        """Add to table flow to match by id and forward to this port"""
        dp = self.dps[dpid]
        parser = dp.ofproto_parser
        match = parser.OFPMatch(mpls_label=self.graph.nodes[port.dpid])
        actions = [parser.OFPActionOutput(port.port_no)]
        self.add_flow(dp, 10, match, actions, table_id=1)
        self.add_mpls_pop(dp)

    def send_arp_mod(self, dp: Datapath, port):
        match = dp.ofproto_parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, eth_dst=port.hw_addr)
        actions = [
            dp.ofproto_parser.OFPActionOutput(dp.ofproto.OFPP_CONTROLLER,dp.ofproto.OFPCML_NO_BUFFER)
        ]
        instructions = [
            dp.ofproto_parser.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS, actions=actions)
        ]
        mod = dp.ofproto_parser.OFPFlowMod(datapath=dp, match=match, instructions=instructions)
        dp.send_msg(mod)

    def send_arp(self, dp: Datapath, port):
        for dst_id in range(1, 20):
            actions = [dp.ofproto_parser.OFPActionOutput(port.port_no)]
            out = dp.ofproto_parser.OFPPacketOut(
                datapath=dp,
                buffer_id=dp.ofproto.OFP_NO_BUFFER,
                in_port=dp.ofproto.OFPP_CONTROLLER,
                actions=actions,
                data=utils.build_arp(dst_id)
            )
            dp.send_msg(out)

    @set_ev_cls(topo_event.EventSwitchEnter)
    def new_switch(self, ev: topo_event.EventSwitchEnter):
        dp: Datapath = ev.switch.dp

        if dp.id not in self.graph.nodes:
            self.graph.add_node(dp.id, id=self.id_counter)
            self.dps[dp.id] = dp
            self.id_counter += 1

            for port in dp.ports.values():
                if port.state == 4:
                    self.send_arp_mod(dp, port)
                    self.send_arp(dp, port)

    @set_ev_cls(topo_event.EventLinkAdd)
    def new_link(self, ev: topo_event.EventLinkAdd):
        src: topo_sw.Port = ev.link.src
        dst: topo_sw.Port = ev.link.dst

        if not self.graph.has_edge(src.dpid, dst.dpid):
            self.add_link_flows(src.dpid, dst)
            self.add_link_flows(dst.dpid, src)

            self.graph.add_edge(src.dpid, dst.dpid, weight=1)
            self.dpid_ports[src.dpid][dst.dpid] = src.port_no
            self.dpid_ports[dst.dpid][src.dpid] = dst.port_no

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev: ofp_event.EventOFPPacketIn):
        msg = ev.msg
        src_dp: Datapath = msg.datapath
        ofproto = src_dp.ofproto
        parser = src_dp.ofproto_parser

        pkt = packet.Packet(msg.data)
        eth_pkt = pkt.get_protocol(ethernet.ethernet)

        if eth_pkt.ethertype == ether.ETH_TYPE_ARP and eth_pkt.dst == 'fe:ee:ee:ee:ee:ef':
            print("hello")
            arp_pkt = pkt.get_protocol(arp.arp)
            self.mac_to_dpid[arp_pkt.src_mac] = src_dp.id
            for table_id in [0, 1]:
                match = parser.OFPMatch(eth_dst=arp_pkt.src_mac)
                actions = [parser.OFPActionOutput(msg.match["in_port"])]
                self.add_flow(src_dp, 1, match, actions, table_id=table_id)
                return

        dst: str = eth_pkt.dst

        src_dpid: int = src_dp.id
        dst_dpid: int = self.mac_to_dpid[dst]

        first, path = utils.get_shortest_path(self.graph, src_dpid, dst_dpid)
        nodes = self.graph.nodes

        actions = []

        for point in path:
            actions.extend(utils.construct_mpls(src_dp, nodes[point]["id"]))

        out_port = self.dpid_ports[src_dpid][first]
        actions.append(parser.OFPActionOutput(out_port))

        # construct packet_out message and send it.
        out = parser.OFPPacketOut(
            datapath=src_dp,
            buffer_id=ofproto.OFP_NO_BUFFER,
            in_port=ofproto.OFPP_CONTROLLER, actions=actions,
            data=msg.data
        )
        src_dp.send_msg(out)


