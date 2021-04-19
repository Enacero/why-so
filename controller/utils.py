from typing import List, Tuple
from ryu.ofproto import ether
from ryu.controller.controller import Datapath
from ryu.lib.packet import packet, ethernet, arp
import networkx as nx


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


def build_arp(id: int, port) -> bytearray:
    dst_ip = f"10.0.0.{id}"
    arp_packet = arp.arp(dst_ip=dst_ip, src_mac=port.hw_addr, src_ip="10.0.0.0")
    msg = packet.Packet()
    msg.add_protocol(arp_packet)
    eth_packet = ethernet.ethernet(dst="ff:ff:ff:ff:ff:ff", ethertype=ether.ETH_TYPE_ARP)
    msg.add_protocol(eth_packet)
    msg.serialize()
    return msg.data
