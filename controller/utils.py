from typing import List, Tuple
from ryu.ofproto import ether
from ryu.controller.controller import Datapath
from ryu.lib.packet import packet, ethernet, arp
from ryu.lib import mac
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


def build_arp(id: int) -> bytearray:
    dst_ip = f"10.0.0.{id}"
    e = ethernet.ethernet('ff:ff:ff:ff:ff:ff', 'fe:ee:ee:ee:ee:ef', ether.ETH_TYPE_ARP)
    a = arp.arp(hwtype=1, proto=ether.ETH_TYPE_IP, hlen=6, plen=4,
                opcode=arp.ARP_REQUEST, src_mac='fe:ee:ee:ee:ee:ef', src_ip='10.0.0.100',
                dst_mac='00:00:00:00:00:00', dst_ip=dst_ip)
    p = packet.Packet()
    p.add_protocol(e)
    p.add_protocol(a)
    p.serialize()
    return p.data
