from typing import Dict, List, Tuple
from collections import defaultdict
from ryu.app.simple_switch_14 import SimpleSwitch14
from ryu.ofproto import ofproto_v1_4
from ryu.controller import ofp_event
from ryu.lib.packet import packet, ethernet
from ryu.ofproto import ether
from ryu.controller.controller import Datapath
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.topology import event as topo_event, switches as topo_sw
import networkx as nx

# TODO:
# Test remove mpls labels on switch


def construct_mpls(dp: Datapath, mpls_id: int) -> List:
    f = dp.ofproto_parser.OFPMatchField.make(
        dp.ofproto.OXM_OF_MPLS_LABEL, mpls_id)

    actions = [dp.ofproto_parser.OFPActionPushMpls(ether.ETH_TYPE_MPLS),
               dp.ofproto_parser.OFPActionSetField(f)]
    return actions


def get_shortest_path(graph: nx.Graph, src: int, dst: int) -> Tuple[int, List[int]]:
    path = nx.shortest_path(graph, src, dst, weight="weight")[1:]
    first = path[0]
    path.reverse()
    return first, path[:-1]


class Controller(SimpleSwitch14):
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


    def add_flow(self, datapath, priority, match, actions, table_id=0):
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

    @set_ev_cls(topo_event.EventSwitchEnter)
    def new_switch(self, ev: topo_event.EventSwitchEnter):
        dp: Datapath = ev.switch.dp
        parser = dp.ofproto_parser

        if dp.id not in self.graph.nodes:
            self.graph.add_node(dp.id, id=self.id_counter)
            self.dps[dp.id] = dp
            self.id_counter += 1

            for table_id in [0, 1]:
                for port in dp.ports.values():
                    self.mac_to_dpid[port.hw_addr] = dp.id
                    match = parser.OFPMatch(eth_dst=port.hw_addr)
                    actions = [parser.OFPActionOutput(port.port_no)]
                    self.add_flow(dp, 1, match, actions, table_id=table_id)

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
        dst: str = eth_pkt.dst

        src_dpid: int = src_dp.id
        dst_dpid: int = self.mac_to_dpid[dst]

        first, path = get_shortest_path(self.graph, src_dpid, dst_dpid)
        nodes = self.graph.nodes

        actions = []

        for point in path:
            actions.extend(construct_mpls(src_dp, nodes[point]["id"]))

        out_port = self.dpid_ports[src_dpid][first]
        actions.append(parser.OFPActionOutput(out_port))

        # construct packet_out message and send it.
        out = parser.OFPPacketOut(datapath=src_dp,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER, actions=actions,
                                  data=msg.data)
        src_dp.send_msg(out)
