from ryu.app.simple_switch_14 import SimpleSwitch14
from ryu.ofproto import ofproto_v1_4
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER
from ryu.controller import ofp_event
from ryu.topology import event as topo_event
import networkx as nx
from matplotlib import pyplot as plt


class Controller(SimpleSwitch14):
    OFP_VERSIONS = [ofproto_v1_4.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph = nx.Graph()
        self.id_counter = 1

    @set_ev_cls(topo_event.EventSwitchEnter)
    def new_switch(self, ev: topo_event.EventSwitchEnter):
        self.graph.add_node(ev.switch.dp.id, id=self.id_counter)
        self.id_counter += 1
        if self.id_counter == 2:
            nx.draw_networkx(self.graph, with_labels=True)
            plt.show()

