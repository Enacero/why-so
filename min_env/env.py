import json
import os
import time
from mininet.util import custom
from mininet.topo import Topo
from mininet.log import setLogLevel, info, debug
from mininet.net import Mininet
from mininet.node import (RemoteController, OVSController, CPULimitedHost)
from mininet.link import TCLink, TCIntf

from .minished import scheduler

EVENTS_FILE = 'events.json'

NODES_NUMBER = 10
SWITCH_NUMBER = 6

LINKS_SWITCH_SWITCH = {
    1: [{'switch': 2, 'bw': 1000, 'loss': 2, 'delay': '2ms',
         'max_queue_size': 30},
        {'switch': 3, 'bw': 10, 'loss': 10, 'delay': '5ms',
         'max_queue_size': 30},
        {'switch': 4, 'bw': 1000, 'loss': 5, 'delay': '20ms',
         'max_queue_size': 30}],
    4: [{'switch': 5, 'bw': 1000, 'loss': 5, 'delay': '20ms',
         'max_queue_size': 30}],
    5: [{'switch': 6, 'bw': 100, 'loss': 10, 'delay': '150ms',
         'max_queue_size': 30}]}

LINKS_SWITCH_HOST = {
    2: [{'host': 1, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 3, 'bw': 20, 'loss': 1, 'delay': '5ms',
         'max_queue_size': 10}],
    3: [{'host': 2, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 7, 'bw': 10, 'loss': 2, 'delay': '5ms',
         'max_queue_size': 10}],
    5: [{'host': 5, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 8, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 10, 'bw': 10, 'loss': 2, 'delay': '5ms',
         'max_queue_size': 10}],
    6: [{'host': 4, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 6, 'bw': 10, 'loss': 2, 'delay': '5ms', 'max_queue_size': 10},
        {'host': 9, 'bw': 10, 'loss': 2, 'delay': '5ms',
         'max_queue_size': 10}]}

TEST_PAIRS = [
    (1, 4),
]


class CustomTopo(Topo):
    def __init__(self, **params):
        super(CustomTopo, self).__init__(**params)
        host_list = [self.addHost('h{}'.format(h), cpu=.1 / NODES_NUMBER)
                     for h in range(1, NODES_NUMBER + 1)]

        switch_list = [self.addSwitch('s{}'.format(s))
                       for s in range(1, SWITCH_NUMBER + 1)]

        for switch, hosts in LINKS_SWITCH_HOST.items():
            for host in hosts:
                linkopts = dict(
                    bw=host['bw'], delay=host['delay'], loss=host['loss'],
                    max_queue_size=host['max_queue_size'])
                self.addLink(
                    host_list[host['host'] - 1], switch_list[switch - 1],
                    **linkopts)

        for switch, links in LINKS_SWITCH_SWITCH.items():
            for link in links:
                linkopts = dict(
                    bw=link['bw'], delay=link['delay'], loss=link['loss'],
                    max_queue_size=link['max_queue_size']
                )
                self.addLink(
                    switch_list[switch - 1], switch_list[link['switch'] - 1],
                    **linkopts)


class Emulation(Mininet):
    def __init__(self, events_file=None, *args, **kwargs):
        super(Emulation, self).__init__(*args, **kwargs)
        self.scheduler = scheduler(time.time, time.sleep)
        if events_file:
            json_events = json.load(open(events_file))
            self.load_events(json_events)

    def load_events(self, json_events):
        for event in json_events:
            event_type = event['type']
            if 'bunch' in event_type:
                getattr(self, event_type)(event)
                continue
            debug("processing event: time "
                  "{time}, type {type}, params {params}\n".format(**event))
            self.scheduler.enter(event['time'], 1, getattr(self, event_type),
                                 kwargs=event['params'])

    def test_network(self, **kwargs):
        """
        Call iperf each host on each host
        :param kwargs: named arguments
            :hosts: list of all hosts in network
            :duration: duration of iperf
        """
        mapper = {
            'tcp': ('iperf -s -i 1 -t {} -P {} -y C',
                    'iperf -t {duration} -c {server_ip}'),
            'udp': ('iperf -u -s -i 1 -t {} -P {} -y C',
                    'iperf -u -t {duration} -c {server_ip} -b {bw}')
        }
        kwargs.setdefault('protocol', 'tcp')
        kwargs.setdefault('duration', 10)
        kwargs.setdefault('bw', 100000)
        info('***iperf event at t={time}: {args}\n'.format(time=time.time(),
                                                           args=kwargs))
        server_cmd, client_cmd = mapper[kwargs['protocol']]

        if not os.path.exists("output"):
            os.makedirs("output")
        hosts = kwargs.pop('hosts')
        template = 'output/iperf-{protocol}-server-{host}.txt'
        filenames = [template.format(host=host, **kwargs) for host in hosts]
        host_objects = [self.get(host) for host in hosts]
        for (server, filename) in zip(host_objects, filenames):
            cmd = server_cmd.format(kwargs['duration'], len(hosts) - 1)
            server.sendCmd('{} &>{} &'.format(cmd, filename))
            server.waiting = False
        time.sleep(2)
        for server in host_objects:
            for client in host_objects:
                if not server == client:
                    cmd = client_cmd.format(server_ip=server.IP(), **kwargs)
                    client.sendCmd('{} &>/dev/null &'.format(cmd))
                    client.waiting = False

    def iperf(self, **kwargs):
        """
        Command to start a transfer between src and dst.
        :param kwargs: named arguments
            src: name of the source node.
            dst: name of the destination node.
            protocol: tcp or udp (default tcp).
            duration: duration of the transfert in seconds (default 10s).
            bw: for udp, bandwidth to send at in bits/sec (default 1 Mbit/sec)
        """
        kwargs.setdefault('protocol', 'TCP')
        kwargs.setdefault('duration', 10)
        kwargs.setdefault('bw', 100000)
        info('***iperf event at t={time}: {args}\n'.format(time=time.time(),
                                                           args=kwargs))

        if not os.path.exists("output"):
            os.makedirs("output")
        server_output = "output/iperf-{protocol}-server-{src}-{dst}.txt".format(
            **kwargs)
        client_output = "output/iperf-{protocol}-client-{src}-{dst}.txt".format(
            **kwargs)
        info('output filenames: {client} {server}\n'.format(
            client=client_output, server=server_output))

        client, server = self.get(kwargs['src'], kwargs['dst'])
        if kwargs['protocol'].upper() == 'UDP':
            iperf_server_cmd = 'iperf -u -s -i 1 -t {} -P 1'.format(
                kwargs['duration'] + 2)
            iperf_client_cmd = 'iperf -u -t {duration} -c {server_ip} -b {bw}'.format(
                server_ip=server.IP(), **kwargs)

        elif kwargs['protocol'].upper() == 'TCP':
            iperf_server_cmd = 'iperf -s -i 1 -t {} -P 1'.format(
                kwargs['duration'] + 2)
            iperf_client_cmd = 'iperf -t {duration} -c {server_ip}'.format(
                server_ip=server.IP(), **kwargs)
        else:
            raise Exception('Unexpected protocol:{protocol}'.format(**kwargs))

        server.sendCmd('{cmd} &>{output} &'.format(cmd=iperf_server_cmd,
                                                   output=server_output))
        info('iperf server command: {cmd} -s -i 1 &>{output} &\n'.format(
            cmd=iperf_server_cmd,
            output=server_output))
        # This is a patch to allow sendingCmd while iperf is running
        # in background.CONS: we can not know when
        # iperf finishes and get their output
        server.waiting = False

        if kwargs['protocol'].lower() == 'tcp':
            while 'Connected' not in client.cmd(
                    'sh -c "echo A | telnet -e A %s 5001"' % server.IP()):
                info('Waiting for iperf to start up...\n')
                time.sleep(.5)

        info('iperf client command: {cmd} &>{output} &\n'.format(
            cmd=iperf_client_cmd, output=client_output))
        client.sendCmd('{cmd} &>{output} &'.format(
            cmd=iperf_client_cmd, output=client_output))
        # This is a patch to allow sendingCmd while iperf is running
        # in background.CONS: we  can not know when
        # iperf finishes and get their output
        client.waiting = False

    def start(self):
        super(Emulation, self).start()
        self.scheduler.run()


def main(cpu=.08, remote=False):
    """
    Test link and CPU badwidth limits
    :param cpu: cpu limit as fraction of overall CPU time
    :param remote: True to use remote controller"""
    intf = custom(TCIntf)
    myTopo = CustomTopo()
    host = custom(CPULimitedHost, sched='cfs', cpu=cpu)
    contr = RemoteController if remote else OVSController
    net = Emulation(
        topo=myTopo, intf=intf, host=host, controller=contr,
        link=TCLink, events_file=EVENTS_FILE)
    net.start()


if __name__ == '__main__':
    setLogLevel('info')
    main()