#!/usr/bin/env python3
import argparse
import configparser
import datetime
import itertools
import json
import sys
from collections import Counter, defaultdict, OrderedDict
from copy import deepcopy
from os.path import isfile
from time import sleep
from typing import Union, Any, ClassVar, Optional, Dict, List, Tuple
from abc import ABC, abstractmethod

import redis


class Inventory(ABC):
    """
    inventory api helper
    """

    @abstractmethod
    def get_ip_info(self, ip_addr: str) -> Dict[str, str]:
        """
        return inventory json answer

        :rtype: Dict[str, str]
        :param ip_addr: 127.0.0.1 for example
        :return: prepared dict like {ip: ip, dc: dc, fqdn: fqdn}
        """
        pass


class MyInventory(Inventory):
    """
    inventory api helper
    """

    def get_ip_info(self, ip_addr: str) -> Dict[str, str]:
        """
        return inventory json answer

        :rtype: dict
        :param ip_addr: 127.0.0.1 for example
        :return: prepared dict like {ip: ip, dc: dc, fqdn: fqdn}
        """
        return {"ip": "127.0.0.1", "dc": "DC1", "fqdn": "fqdn"}


class RedisClusterTool:
    """
    simple class for redis cluster tooling
    """

    MAXPORT: ClassVar[int] = 65535
    SKEW: ClassVar[int] = 5
    REPLICAS: ClassVar[int] = 2

    def __repr__(self):
        return f'RedisClusterTool connected to {self.host}:{self.port}'

    def __init__(self, host: str, port: int, passwd: str, skipconnection: bool = False, onlyconnected: bool = False):
        """
        initial func

        :param host: host to connect to redis cluster
        :param port: port for connect to redis cluster
        :param skipconnection: don't connect to redis server
        :param onlyconnected: not use disconnected node
        """
        self.host: str = host
        self.port: int = port
        if not skipconnection:
            self.rc: redis.RedisCluster = redis.RedisCluster(host=self.host, port=self.port, password=passwd)
            self.currentnodes = self.get_current_nodes(onlyconnected=onlyconnected)
        self.plans = list()

    def levelout_masters(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> List[Dict[str, Any]]:
        """
        Levelout masters before rebalancing
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: planned nodes
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)

        # determine how much masters per group should be
        group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        groups = sorted(group_nodes.keys())
        masters = self.get_masters(nodes=nodes, maxport=maxport)
        floor, remainder = len(masters) // len(groups), len(masters) % len(groups)
        desired_groups_len = OrderedDict(map(lambda group: (group, floor), groups))
        for index in range(remainder):
            desired_groups_len[groups[index]] += 1
        desired_nodes_skew = dict()
        for group, number in desired_groups_len.items():   # there can't be more masters than nodes
            if number > len(group_nodes[group]):
                desired_nodes_skew[group] = len(group_nodes[group])
        for group, actual_nodes_amount in desired_nodes_skew.items():
            skew = desired_groups_len[group] - actual_nodes_amount
            desired_groups_len[group] = actual_nodes_amount
            while skew != 0:
                # iterate over sorted by number desired groups len over that not includes skew groups
                for desired_nodes_group in list(filter(lambda group: group not in set(desired_nodes_skew.items()),
                                                       dict(sorted(desired_groups_len.items(), key=lambda num: num[1])).keys())):
                    if len(group_nodes[desired_nodes_group]) != desired_groups_len[desired_nodes_group]:
                        desired_groups_len[desired_nodes_group] += 1
                        skew -= 1
                        if skew == 0:
                            break

        # rebalance masters
        for group in groups:
            group_masters = self.get_masters(nodes=group_nodes[group], maxport=maxport)
            master_skew = desired_groups_len[group] - len(group_masters)

            if master_skew > 0:  # need to get more masters (too low number of masters)
                for _ in range(0, master_skew):
                    success = False
                    neighbor_nodes_groups: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] != group, group_nodes.items()))
                    for neighbor_group, neighbor_nodes in neighbor_nodes_groups.items():
                        neighbor_group_masters = self.get_masters(nodes=neighbor_nodes, maxport=maxport)
                        if len(neighbor_group_masters) > desired_groups_len[neighbor_group]:
                            for slave_node in self.get_slaves(nodes=group_nodes[group], maxport=maxport):
                                if self.get_node_group(nodes=nodes,
                                                       node=self.get_masters(nodes=nodes, slavenodeid=slave_node['node_id'], maxport=maxport),
                                                       maxport=maxport) == neighbor_group:  # orphaned nodes can't exist
                                    nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slave_node['node_id'])
                                    group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                    success = True
                                    break  # stop iterate over current group slaves
                            if success:
                                break  # stop iterate over available groups
                    if success:
                        continue  # continue if we find appropriate node for failover
                    # if we here - we were failed to find
                    for neighbor_group, neighbor_nodes in neighbor_nodes_groups.items():
                        neighbor_group_masters = self.get_masters(nodes=neighbor_nodes, maxport=maxport)
                        if len(neighbor_group_masters) > desired_groups_len[neighbor_group]:
                            group_slaves = self.get_slaves(nodes=group_nodes[group], maxport=maxport)
                            if group_slaves:
                                nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=neighbor_group_masters[-1]['node_id'], slavenodeid=group_slaves[0]['node_id'])
                                nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=group_slaves[0]['node_id'])
                                group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                break
            elif master_skew < 0:  # need to reduce masters (too high number of masters)
                for _ in range(master_skew, 0):
                    success = False
                    neighbor_nodes_groups: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] != group, group_nodes.items()))
                    for neighbor_group, neighbor_nodes in neighbor_nodes_groups.items():
                        neighbor_group_masters = self.get_masters(nodes=neighbor_nodes, maxport=maxport)
                        if len(neighbor_group_masters) < desired_groups_len[neighbor_group]:
                            for slave_node in self.get_slaves(nodes=group_nodes[neighbor_group], maxport=maxport):
                                if self.get_node_group(nodes=nodes,
                                                       node=self.get_masters(nodes=nodes, slavenodeid=slave_node[
                                                           'node_id'], maxport=maxport),
                                                       maxport=maxport) == group:
                                    nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slave_node['node_id'])
                                    group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                    group_masters = self.get_masters(nodes=group_nodes[group], maxport=maxport)
                                    success = True
                                    break  # stop iterate over current group slaves
                            if success:
                                break  # stop iterate over available groups
                    if success:
                        continue  # continue if we find appropriate node for failover
                    # if we here - we were failed to find, will replicate nodes
                    for neighbor_group, neighbor_nodes in neighbor_nodes_groups.items():
                        neighbor_group_masters = self.get_masters(nodes=neighbor_nodes, maxport=maxport)
                        if len(neighbor_group_masters) < desired_groups_len[neighbor_group]:
                            neighbor_group_slaves = self.get_slaves(nodes=neighbor_nodes, maxport=maxport)
                            if neighbor_group_slaves:
                                nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=group_masters[-1]['node_id'], slavenodeid=neighbor_group_slaves[0]['node_id'])
                                nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=neighbor_group_slaves[0]['node_id'])
                                group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                group_masters = self.get_masters(nodes=group_nodes[group], maxport=maxport)
                                break
            else:  # zero skew
                continue  # nothing to do

        return nodes

    def levelout_slaves(self, nodes: List[Dict[str, Any]] = None, replicas: int = REPLICAS, maxport: int = MAXPORT) -> List[Dict[str, Any]]:
        """
        Levelout slaves before rebalancing
        :param nodes: nodes list
        :param replicas: desired number of replicas
        :param maxport: reduce ports to maximum value
        :return: planned nodes
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)

        # level out slaves
        indexes_for_remove = []
        # step 1 - remove fine leveled nodes from nodes
        masters = self.get_masters(nodes=nodes, maxport=maxport)
        for master in masters:
            master_group = self.get_node_group(nodes=nodes, maxport=maxport, nodeid=master['node_id'])
            slaves = self.get_slaves(nodes=nodes, masternodeid=master['node_id'])
            local_workset_groups = self.get_nodes_groups(nodes=[master] + slaves, maxport=maxport)
            if len(slaves) == replicas and len(local_workset_groups) == replicas + 1:
                for node in [master]+slaves:
                    indexes_for_remove.append(self.get_node_index(nodes=nodes, nodeid=node['node_id']))
            elif len(slaves) > replicas and len(local_workset_groups) == replicas + 1:
                for group, group_slaves in self.get_nodes_groups(nodes=slaves, maxport=maxport).items():
                    if group != master_group:
                        indexes_for_remove.append(self.get_node_index(nodes=nodes, nodeid=group_slaves[-1]['node_id']))
                indexes_for_remove.append(self.get_node_index(nodes=nodes, nodeid=master['node_id']))
            else:
                continue   # keep unclean nodes in workset

        # get set of masters without correct slave set and wrongly connected slaves that we can use
        workset_nodes = deepcopy(nodes)
        for index in sorted(indexes_for_remove, reverse=True):
            workset_nodes.pop(index)

        # step 2 - connect non-fine leveled nodes between each other using groups
        workset_masters = self.get_masters(nodes=workset_nodes, maxport=maxport)
        for master in workset_masters:
            workset_slaves = self.get_slaves(nodes=workset_nodes, maxport=maxport)
            workset_slaves_ids = list(map(lambda node: node['node_id'], workset_slaves))
            master_group = self.get_node_group(nodes=nodes, nodeid=master['node_id'], maxport=maxport)

            workset_slaves_groups = self.get_nodes_groups(nodes=workset_slaves, maxport=maxport)
            workset_slaves_groups_wo_mg = dict(filter(lambda kv: kv[0] != master_group, workset_slaves_groups.items()))

            # try softly (with less amount of cluster replicates) swap slaves with neighbors if we can't find enough number of groups
            workset_masters_ids: List[str] = list(map(lambda node: node['node_id'], workset_masters))
            if len(workset_slaves_groups_wo_mg.keys()) < replicas:
                # filter only healthy masters that not exists in our problem workset
                neighbor_group_masters: List[Dict[str, Any]] = list(filter(lambda node: node['node_id'] not in workset_masters_ids and self.get_node_group(nodes=nodes, node=node) != master_group,
                                                                 self.get_masters(nodes=nodes, maxport=maxport)))  # only reason is get healthy masters from another groups
                for neighbor_master in neighbor_group_masters:
                    neighbor_master_group = self.get_node_group(nodes=nodes, maxport=maxport, node=neighbor_master)
                    neighbor_master_slaves: List[Dict[str, Any]] = list(filter(lambda node: node['node_id'] not in workset_slaves_ids, self.get_slaves(nodes=nodes, masternodeid=neighbor_master['node_id'], maxport=maxport)))
                    neighbor_master_slaves_groups: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] != master_group,   # filter inappropriate slaves that in the same group as our master
                                                                           self.get_nodes_groups(nodes=neighbor_master_slaves, maxport=maxport).items()))
                    slaves_group_diff = set(neighbor_master_slaves_groups).symmetric_difference(workset_slaves_groups_wo_mg.keys())
                    if slaves_group_diff:
                        for neighbor_master_slave_group, neighbor_master_slaves in dict(filter(lambda kv: kv[0] in slaves_group_diff, neighbor_master_slaves_groups.items())).items():   # iterate over neighbor master slaves that exists in diff group
                            if neighbor_master_slave_group not in (set(workset_slaves_groups_wo_mg) | {master_group}):   # check that suggested neighbor slave not exists in our master or slave groups
                                # choose which slave we change
                                slaves_for_change: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] in slaves_group_diff                  # slave group should be in our diff groups
                                                                                  and kv[0] != neighbor_master_group                     # slave group shouldn't be equal neighbor master group
                                                                                  and kv[0] not in set(neighbor_master_slaves_groups),   # slave group shouldn't already exist in neighbour master slaves
                                                                                  workset_slaves_groups_wo_mg.items()))                  # candidates that we can give to neighbor master for his slave
                                if slaves_for_change:   # continue if we find some appropriate slaves for swap
                                    slave_for_change = self.mergevalueslists(*slaves_for_change.values())[0]

                                    nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=master['node_id'],
                                                                            slavenodeid=neighbor_master_slaves[0]['node_id'])
                                    nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=neighbor_master['node_id'],
                                                                            slavenodeid=slave_for_change['node_id'])
                                    workset_nodes[self.get_node_index(nodes=workset_nodes, nodeid=slave_for_change['node_id'])] = neighbor_master_slaves[0]
                                    workset_slaves = self.get_slaves(nodes=workset_nodes, maxport=maxport)
                                    workset_slaves_groups = self.get_nodes_groups(nodes=workset_slaves, maxport=maxport)
                                    workset_slaves_groups_wo_mg: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] != master_group, workset_slaves_groups.items()))
                                    neighbor_master_slaves: List[Dict[str, Any]] = list(filter(lambda node: node['node_id'] not in workset_slaves_ids, self.get_slaves(nodes=nodes,
                                                                                     masternodeid=neighbor_master['node_id'], maxport=maxport)))
                                    neighbor_master_slaves_groups: Dict[str, List[Dict[str, Any]]] = dict(filter(lambda kv: kv[0] != master_group,  # filter inappropriate slaves that in the same group as our master
                                                                                                  self.get_nodes_groups(nodes=neighbor_master_slaves, maxport=maxport).items()))
                                    if len(workset_slaves_groups_wo_mg) >= replicas:
                                        break
                        if len(workset_slaves_groups_wo_mg) >= replicas:
                            break
                    else:
                        continue
            # check success and do more aggressive if we can't find appropriate set of slave nodes (add all another nodes until success)
            if len(workset_slaves_groups_wo_mg) < replicas:
                while len(workset_slaves_groups_wo_mg) < replicas and len(workset_masters) != masters:
                    neighbor_group_masters = list(filter(lambda node: node['node_id'] not in workset_masters_ids,
                                                         self.get_masters(nodes=nodes, maxport=maxport)))
                    neighbor_master_slaves = self.get_slaves(nodes=nodes, masternodeid=neighbor_group_masters[0]['node_id'])
                    for node in [neighbor_group_masters[0]] + neighbor_master_slaves:
                        if not self.get_node(nodes=workset_nodes, nodeid=node['node_id']):   # add to work workset only once
                            workset_nodes.append(node)
                    workset_masters.append(neighbor_group_masters[0])   # but we need to handle this master again in any case
                    workset_masters_ids.append(neighbor_group_masters[0]['node_id'])
                    workset_slaves = self.get_slaves(nodes=workset_nodes, maxport=maxport)
                    workset_slaves_ids = list(map(lambda node: node['node_id'], workset_slaves))
                    workset_slaves_groups = self.get_nodes_groups(nodes=workset_slaves, maxport=maxport)
                    workset_slaves_groups_wo_mg = dict(filter(lambda kv: kv[0] != master_group, workset_slaves_groups.items()))

            # check success
            if len(workset_slaves_groups_wo_mg) < replicas:
                raise Exception(f"Can't find required {replicas} groups for master {self.get_node_group(nodes=nodes, nodeid=master['node_id'], maxport=maxport)} {master['node_id']} {master['host']}:{master['port']}")

            for group in tuple(workset_slaves_groups_wo_mg.keys())[:replicas]:
                workset_slaves = self.get_slaves(nodes=workset_nodes, maxport=maxport)
                workset_slaves_groups = self.get_nodes_groups(nodes=workset_slaves, maxport=maxport)
                slave_for_replicate = workset_slaves_groups[group][0]
                nodes = self.plan_clusternode_replicate(nodes=nodes, slavenodeid=slave_for_replicate['node_id'], masternodeid=master['node_id'])
                workset_nodes.pop(self.get_node_index(nodes=workset_nodes, nodeid=slave_for_replicate['node_id']))
        return nodes

    def create_command(self, command: str, run_node: Dict[str, Any], affected_node: Dict[str, Any], args: Union[tuple, List] = tuple(),
                       command_option: str = "") -> Dict[str, Any]:
        """
        Construct a redis command for clusterexecute function with description

        :param command: command to run on redis
        :param run_node: node where to execute command
        :param affected_node: node that will be affected
        :param args: arguments for clusterexecute func
        :param command_option: optional argument for command such as TAKEOVER / FORCE
        :return: command in {func, args, kwargs, msg} format
        """

        if command == 'CLUSTER REPLICATE':
            exec_command = 'CLUSTER REPLICATE ' + affected_node['node_id']
            command_desc = f'Attach slave  {run_node["id"]} {run_node["host"]}:{run_node["port"]} ' \
                           f'to {affected_node["id"]} {affected_node["host"]}:{affected_node["port"]}'

        elif command == 'CLUSTER FAILOVER':
            if command_option == "":
                exec_command = 'CLUSTER FAILOVER'
            else:
                exec_command = 'CLUSTER FAILOVER ' + command_option
            command_desc = f'Failover node {run_node["node_id"]} {run_node["host"]}:{run_node["port"]} ' \
                           f'[old master {affected_node["node_id"]} {affected_node["host"]}:{affected_node["port"]}]'
        else:
            raise Exception(f"Unknown command for redisclustertool: {command}")

        command = {'func': self.cluster_execute, 'args': args,
                   'kwargs': {'ip': run_node['host'],
                              'port': run_node['port'],
                              'command': exec_command},
                   'msg': command_desc
                   }
        return command

    def get_current_nodes(self, onlyconnected: bool = False) -> List[Dict[str, Any]]:
        """
        return current cluster nodes configuration from actual cluster

        :param onlyconnected: not use disconnected node
        :rtype: List[Dict[str, Any]]
        :return: list like {'node_id': 'nodeid' 'host': 'hostip', 'port': someport,
         'flags': ('slave',), 'master_id': 'masternodeid', 'ping-sent': 0, 'pong-recv': 1610468870000,
         'link-state': 'connected', 'slots': [], 'migrations': []}
        """
        prepared_nodes = []
        for host, params in self.rc.cluster_nodes().items():
            host, port = host.split(':')
            params['host'], params['port'] = host, int(port)
            prepared_nodes.append(params)
        if onlyconnected:
            return sorted(self.filter_only_connected_nodes(
                nodes=self.filter_without_noaddr_flag_nodes(nodes=prepared_nodes)
            ),
                key=lambda node: (node['host'], node['port']))
        else:
            return sorted(self.filter_without_noaddr_flag_nodes(nodes=prepared_nodes),
                          key=lambda node: (node['host'], node['port']))

    def filter_only_connected_nodes(self, nodes: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Return nodes with only state connected

        :rtype: List[Dict[str, Any]]
        :param nodes: nodes list
        :return: list with only connected nodes
        """
        if nodes is None:
            nodes = self.currentnodes
        return list(filter(lambda node: node['connected'] == True, nodes))

    def filter_without_noaddr_flag_nodes(self, nodes: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Return nodes without flag noaddr

        :rtype: List[Dict[str, Any]]
        :param nodes: nodes list
        :return: list with only connected nodes
        """
        if nodes is None:
            nodes = self.currentnodes
        return list(filter(lambda node: 'noaddr' not in node['flags'], nodes))

    def get_masters(self, nodes: List[Dict[str, Any]] = None, slavenodeid: str = None, maxport: int = MAXPORT) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        get reduced node list with only masters (or only masters of the slave nodeid if defined) and
        include only instances with port <= maxport

        :rtype: Union[List[Dict[str, Any]], Dict[str, Any]]
        :param nodes: nodes list
        :param slavenodeid: if defined return only master of defined nodeid slave
        :param maxport: reduce ports to maximum value
        :return: reduced node list or masternode if slavenodeid defined
        """
        if nodes is None:
            nodes = self.currentnodes
        if not isinstance(nodes, list):
            raise TypeError(f"Nodes must be list, got {type(nodes)}")
        if slavenodeid:
            slavenode = self.get_node(nodes=nodes, maxport=maxport, nodeid=slavenodeid)
            if 'slave' not in slavenode['flags']:
                raise Exception(f'Provided slavenode {slavenode["id"]} is not slave!')
            masternodes: List[Dict[str, Any]] = list(filter(lambda x: x['node_id'] == slavenode['master_id'], self.nodes_reduced_max_port(nodes=nodes, maxport=maxport)))
            if masternodes:
                return masternodes[0]
            else:
                return masternodes
        return list(filter(lambda node: 'master' in node.get('flags'),
                           self.nodes_reduced_max_port(nodes=nodes, maxport=maxport)))

    def get_slaves(self, nodes: List[Dict[str, Any]] = None, masternodeid: str = None, maxport: int = MAXPORT) -> List[Dict[str, Any]]:
        """
        get reduced node list with only slaves (or only slaves of the master nodeid if defined) and
        include only instances with port <= maxport

        :rtype: List[Dict[str, Any]]
        :param nodes: nodes list
        :param masternodeid: if defined return only slaves of defined nodeid master
        :param maxport: reduce ports to maximum value
        :return: reduced node list
        """
        if nodes is None:
            nodes = self.currentnodes
        if not isinstance(nodes, list):
            raise TypeError(f"Nodes must be list, got {type(nodes)}")
        if masternodeid:
            return list(filter(lambda node: node['master_id'] == masternodeid,
                               self.nodes_reduced_max_port(nodes=nodes, maxport=maxport)))
        else:
            return list(
                filter(lambda node: 'slave' in node['flags'],
                       self.nodes_reduced_max_port(nodes=nodes, maxport=maxport)))

    def get_node(self, nodeid: Union[str, List[str]], nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        return selected nodeids

        :rtype: Union[Dict[str, Any], List[Dict[str, Any]]]
        :param nodeid: nodeid or list of nodeids
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: node of nodeid or nodes list if nodeid is list with nodeids
        """
        if nodes is None:
            nodes = self.currentnodes
        if not isinstance(nodes, list):
            raise TypeError(f"Nodes must be list, got {type(nodes)}")

        if isinstance(nodeid, str):
            for node in self.nodes_reduced_max_port(nodes=nodes, maxport=maxport):
                if node['node_id'] == nodeid:
                    return node
        elif isinstance(nodeid, list):
            nodeslist: List = list()
            for ID in nodeid:
                for node in self.nodes_reduced_max_port(nodes=nodes, maxport=maxport):
                    if node['node_id'] == ID:
                        nodeslist.append(node)
            return nodeslist

    def get_max_port(self, nodes: List[Dict[str, Any]] = None) -> int:
        """
        get highest port in nodes list

        :rtype: int
        :param nodes:  nodes list
        :return: highest port
        """
        if nodes is None:
            nodes = self.currentnodes
        return max(map(lambda node: node['port'], nodes))

    def get_min_port(self, nodes: List[Dict[str, Any]] = None) -> int:
        """
        get lowest port in nodes list

        :rtype: int
        :param nodes:  nodes list
        :return: lowest port
        """
        if nodes is None:
            nodes = self.currentnodes
        return min(map(lambda node: node['port'], nodes))

    def get_servers_count(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> int:
        """
        get cluster servers count

        :rtype: int
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: count servers
        """
        if nodes is None:
            nodes = self.currentnodes
        return len(self.get_server_ips(nodes=nodes, maxport=maxport))

    def get_server_ips(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> List[str]:
        """
        get all servers ips

        :rtype: set
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: server ips set([ip1, ip2])
        """
        if nodes is None:
            nodes = self.currentnodes
        return sorted(set(map(lambda node: node['host'], self.nodes_reduced_max_port(nodes=nodes, maxport=maxport))))

    def get_nodes_groups(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Dict[str, List[Dict[str, Any]]]:
        """
        return nodes places into host groups

        :rtype: Dict[str, List[Dict[str, Any]]]
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: dict like {'group1': [node1,node2], 'group2': [node3, node4]}
        """
        if nodes is None:
            nodes = self.currentnodes
        if not isinstance(nodes, list):
            raise TypeError(f"Nodes must be list, got {type(nodes)}")
        nodesgroup: defaultdict = defaultdict(list)
        for node in self.nodes_reduced_max_port(maxport=maxport, nodes=nodes):
            nodesgroup[node['host']].append(node)
        return nodesgroup

    def get_node_group(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, node: Dict[str, Any] = None, nodeid: str = None) -> str:
        """
        return group of given node or nodeid

        :rtype: str
        :param node: node list
        :param nodeid: id of node from node definition
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: group name as string
        """
        if nodes is None:
            nodes = self.currentnodes
        if (node and nodeid) or (not node and not nodeid):
            raise Exception('You mast give only node parameter or nodeid parameter')
        if node:
            nodeid = node['node_id']
        nodesgroups = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        for group, groupnodes in nodesgroups.items():
            if self.get_node(nodes=groupnodes, maxport=maxport, nodeid=nodeid):
                return group

    def check_distribution_possibility(self, replicas: int = REPLICAS, nodes: List[Dict[str, Any]] = None,
                                       maxport: int = MAXPORT) -> bool:
        """
        Simple check that all servers can be distributed over available servers (or server group)

        :rtype: bool
        :param replicas: desired number of replicas
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: True if all master-slave group can be distributed over different groups else False
        """
        if nodes is None:
            nodes = self.currentnodes
        nodesgroups = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        nodesgroupscounter: Counter = Counter(dict(map(lambda kv: (kv[0], len(kv[1])), nodesgroups.items())))

        for group in range(len(self.get_masters(nodes=nodes))):
            if len(nodesgroupscounter) < replicas + 1:
                return False
            nodesgroupscounter.subtract(
                Counter(dict(map(lambda kv: (kv[0], 1), nodesgroupscounter.most_common()[:replicas + 1]))))
            nodesgroupscounter = +nodesgroupscounter  # remove zero and negative numbers
        return True

    def nodes_reduced_max_port(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> List[Dict[str, Any]]:
        """
        return reduced nodes list with excluded nodes with port > than maxport

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: reduced node list
        """
        if nodes is None:
            nodes = self.currentnodes
        if not isinstance(nodes, list):
            raise TypeError(f"Nodes must be list, got {type(nodes)}")
        filtered_node = list(filter(lambda node: node['port'] <= maxport, nodes))
        return filtered_node

    def check_masterslave_in_group(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, replicas: int = REPLICAS) -> Dict[str, List[Dict[str, Union[Dict[str, Any], List[Dict[str, Any]]]]]]:
        """
        Check if master and slave are located in one group (server)

        :rtype: Dict[str, List[Dict[str, Union[Dict[str, Any], List[Dict[str, Any]]]]]]
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param replicas: desired number of replicas
        :return: dict list {'group': [ {'master': masternode, 'slaves': [slavenode1, slavenode2...] } ] }
        """
        if nodes is None:
            nodes = self.currentnodes
        distribution_problem: defaultdict = defaultdict(list)

        for group, groupnodes in self.get_nodes_groups(nodes=nodes, maxport=maxport).items():
            for masternode in self.get_masters(groupnodes):
                # get list of slave nodes of defined master on this group
                slave_nodes_of_master_nodeid = self.get_slaves(nodes=nodes, masternodeid=masternode['node_id'],
                                                               maxport=maxport)
                # skip check if master has enough slaves, it's not a problem
                slave_nodes_groups = list(map(lambda node: self.get_node_group(nodes=nodes, maxport=maxport, node=node),
                                              slave_nodes_of_master_nodeid))
                master_node_group = self.get_node_group(nodes=nodes, maxport=maxport, node=masternode)
                slave_nodes_groups_reduced = list(filter(lambda group: group != master_node_group, slave_nodes_groups))
                if master_node_group in slave_nodes_groups and len(slave_nodes_groups_reduced) < replicas:
                    distribution_problem[group].append({'master': masternode, 'slaves': slave_nodes_of_master_nodeid})
        return distribution_problem

    def check_slavesofmaster_in_group(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT,
                                      replicas: int = REPLICAS) -> Dict[str, List[Union[Dict[str, Any], List[Dict[str, Any]]]]]:
        """
        Check group that doesn't have too many slaves of one master
        for prevent situation that all slaves will down with group disaster

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param replicas: desired number of replicas
        :return: dict list {'group_with_slaves': [ {'master': masternode, 'slaves': [slavenode1, slavenode2...] } ] }
        """
        if nodes is None:
            nodes = self.currentnodes
        distribution_problem: defaultdict = defaultdict(list)

        # if we can't distribute all replicas to a different DC, it means that we have a lot of replicas and this not a problem
        if not self.check_distribution_possibility(nodes=nodes, replicas=replicas, maxport=maxport):
            return distribution_problem

        nodesgroups = self.get_nodes_groups(nodes=nodes)

        for group, groupnodes in nodesgroups.items():
            # all slaves nodeid in group
            groupslavenodes: list = self.get_slaves(nodes=groupnodes, maxport=maxport)
            # get all master's nodeids from slaves node definition
            master_nodeids_of_groupslavenodes: List[str] = sorted(
                set(filter(None, map(lambda nodeid: nodeid['master_id'], groupslavenodes))))
            # find all master and slaves in group and append to distribution_problem dict
            for master_nodeid in master_nodeids_of_groupslavenodes:
                slaves = self.get_slaves(nodes=groupnodes, maxport=maxport, masternodeid=master_nodeid)
                # check that global groups counts with slave of master_nodeid => 0
                slaves_of_master = self.get_slaves(nodes=nodes, maxport=maxport, masternodeid=master_nodeid)
                # if all required replicas in different groups - it's not a problem
                if len(self.get_nodes_groups(nodes=slaves_of_master, maxport=maxport)) >= replicas:
                    continue
                # it's problem if group has too many replicas of one master
                if len(slaves) > 1:
                    masternode = self.get_node(nodeid=master_nodeid, nodes=nodes, maxport=maxport)
                    distribution_problem[group].append({'master': masternode, 'slaves': slaves})
        return distribution_problem

    def check_slaveofslave(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Tuple[Tuple[Any]]:
        """
        Return tuple with tuples of slaves that slave from slaves and fake master (slave) id

        :param nodes: nodes list
        :param maxport: maxport count
        :return: tuple with tuples (slave, slave_of)
        """
        if nodes is None:
            nodes = self.currentnodes

        problem_pairs = []
        slaves: List[Dict[str, Any]] = self.get_slaves(nodes=nodes, maxport=maxport)
        for slave in slaves:
            master_of_slave = self.get_masters(nodes=nodes, slavenodeid=slave['node_id'], maxport=maxport)
            if 'slave' in master_of_slave['flags']:
                problem_pairs.append(tuple([slave['node_id'], master_of_slave['node_id']]))
        return tuple(problem_pairs)

    def check_failed_nodes(self, nodes: List[Dict[str, Any]] = None) -> Tuple[Any]:
        """
        Return tuple with nodes that have fail flag

        :rtype: Tuple[Dict[str, Any]]
        :param nodes: nodes list
        :return: tuple with tuples (slave, slave_of)
        """
        if nodes is None:
            nodes = self.currentnodes

        return tuple(filter(lambda node: 'fail' in node.get('flags'), nodes))

    def check_group_master_distribution(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, skew: int = SKEW) -> Dict[str, int]:
        """
        Return non-empty dict if max masters count per group and min masters count per group has diff more than skew percents

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param skew: max-min percentage difference
        :return: dict like {group1: group1_masterpercent, group2: group2_masterpercent} if group has disbalance more than skew percent, else empty dict
        """
        if nodes is None:
            nodes = self.currentnodes
        allmastercount: int = len(self.get_masters(nodes=nodes, maxport=maxport))
        master_per_group_percentage: Dict = {
            group: round((100 / allmastercount) * len(self.get_masters(nodes=groupnodes, maxport=maxport)),
                         2) if allmastercount != 0 else 0
            for group, groupnodes in self.get_nodes_groups(nodes=nodes, maxport=maxport).items()}
        percents = self.mergevalueslists(master_per_group_percentage)
        if max(percents) - min(percents) > skew:
            return master_per_group_percentage
        return dict()

    def check_distribution_ok(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, skew: int = SKEW,
                              replicas: int = REPLICAS) -> int:
        """
        check that redis cluster doesn't have master-slave pair in one group or more than one slave in one group

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param skew: max-min percentage difference
        :param replicas: desired number of replicas
        :return: nagios format, 0 if cluster OK, 1 if WARN (just distribution skew problem), 2 if CRITICAL (serious problem with master-slave distribution)
        """
        if nodes is None:
            nodes = self.currentnodes
        if any([self.check_masterslave_in_group(nodes=nodes, maxport=maxport, replicas=replicas),
                self.check_slavesofmaster_in_group(nodes=nodes, maxport=maxport, replicas=replicas),
                self.check_master_does_not_have_desired_replica_count(nodes=nodes, maxport=maxport, replicas=replicas),
                self.check_master_does_not_have_slaves(nodes=nodes, maxport=maxport)]):
            return 2
        if self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=skew):
            return 1
        return 0

    def check_master_without_slots(self, nodes: List[Dict[str, Any]] = None) -> Tuple[Any]:
        """
        check that redis cluster doesn't have masters without slots

        :rtype: Tuple[RedisCluster]
        :param nodes: nodes list
        :return: master nodes without slots
        """
        if nodes is None:
            nodes = self.currentnodes
        master_nodes = self.get_masters(nodes=nodes)
        return tuple(filter(lambda node: not node.get('slots', ()), master_nodes))

    def print_problems(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, skew: int = SKEW,
                       replicas: int = REPLICAS) -> None:
        """
        Function only print all problems with current cluster distributioin

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param skew: max-min percentage difference
        :param replicas: desired number of replicas
        :return: None
        """
        if nodes is None:
            nodes = self.currentnodes

        check_slaveofslave_problem = self.check_slaveofslave(nodes=nodes, maxport=maxport)
        if check_slaveofslave_problem:
            print(f"Cluster has slave of slaves problem ({len(check_slaveofslave_problem)}):")
            for nodeids_pair in check_slaveofslave_problem:
                print(f'Slave node {nodeids_pair[0]} is slave of slave {nodeids_pair[1]}')

        check_masterslave_in_group_problem = self.check_masterslave_in_group(nodes=nodes, maxport=maxport,
                                                                             replicas=replicas)
        if check_masterslave_in_group_problem:
            countproblems = sum(map(lambda problems: len(problems), check_masterslave_in_group_problem.values()))
            print(f'Master-slave distribution problem ({countproblems}):')
            for group, problems in check_masterslave_in_group_problem.items():
                for problem in problems:
                    for slavenode in problem['slaves']:
                        print(f'    Server {group} has master {problem["master_id"]["id"]} '
                              f'{problem["master_id"]["host"]}:{problem["master_id"]["port"]} '
                              f'with slave {slavenode["id"]} {slavenode["host"]}:{slavenode["port"]} '
                              f'on same server')
            print()

        check_slavesofmaster_on_group_problem = self.check_slavesofmaster_in_group(nodes=nodes, maxport=maxport,
                                                                                   replicas=replicas)
        if check_slavesofmaster_on_group_problem:
            print(
                f'Too many slaves of one master in group problems ({sum(len(x) for x in check_slavesofmaster_on_group_problem.values())}):')
            for group, problems in check_slavesofmaster_on_group_problem.items():
                for problem in problems:
                    subj = ' and '.join([f'{slavenode["id"]} {slavenode["host"]}:{slavenode["port"]}' for slavenode in
                                         problem['slaves']])
                    print(f'    Server {problem["master_id"]["host"]} has master {problem["master_id"]["id"]} '
                          f'{problem["master_id"]["host"]}:{problem["master_id"]["port"]} '
                          f'with {len(problem["slaves"])} slaves {subj} placed in one server {group}')
            print()

        check_master_does_not_have_desired_replica_count = self.check_master_does_not_have_desired_replica_count(
            nodes=nodes, maxport=maxport, replicas=replicas)
        if check_master_does_not_have_desired_replica_count:
            print(
                f"Masters don't have desired replica count {replicas} problem ({len(check_master_does_not_have_desired_replica_count)}):")
            for master_node_id, count in check_master_does_not_have_desired_replica_count.items():
                master_node = self.get_node(nodes=nodes, nodeid=master_node_id)
                print(f'    Master node {master_node["id"]} ({master_node["host"]}) has {count} replicas')
            print()

        check_master_does_not_have_slaves: List[str] = self.check_master_does_not_have_slaves(nodes=nodes,
                                                                                              maxport=maxport)
        if check_master_does_not_have_slaves:
            print(f"Masters without slaves problem ({len(check_master_does_not_have_slaves)}):")
            for master_node_id in check_master_does_not_have_slaves:
                master_node = self.get_node(nodes=nodes, nodeid=master_node_id)
                print(f'    Master node {master_node["id"]} ({master_node["host"]}) has no slaves')
            print()

        masters_group_skew: Dict = self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=skew)
        if masters_group_skew:
            print(f'Groups have master distribution skew more than {skew}% (actual '
                  f'{round(max(masters_group_skew.values()) - min(masters_group_skew.values()), 2)}%): {masters_group_skew}\n')

    @staticmethod
    def mergevalueslists(*objects: Union[Dict, list]) -> list:
        """
        Return new list with merged values from objects
        for example:
        a = {'key1': 'value1'}
        b = {'key2': ['value2']}
        c = ['value3']
        d = {"key4": {"dict": "value4"}}
        mergevalueslists(a,b,c,d) -> ['value1', 'value2', 'value3', {'dict': 'value4'}]

        :rtype: List[Any]
        :param objects: any objects that can be added to list
        :return: list of objects
        """
        newlist: list = list()
        extendable: tuple = (list, tuple)
        for obj in objects:
            if isinstance(obj, extendable):
                newlist.extend(obj)
            elif isinstance(obj, dict):
                for value in obj.values():
                    if isinstance(value, extendable):
                        newlist.extend(value)
                    else:
                        newlist.append(value)
            else:
                newlist.append(obj)
        return newlist

    def get_node_index(self, nodeid: str, nodes: List[Dict[str, Any]] = None) -> int:
        """
        get position of nodeid in nodes list

        :rtype: int
        :param nodeid: node identificator
        :param nodes: nodes list
        :return: position in array nodes
        """
        if nodes is None:
            nodes = self.currentnodes
        for index, node in enumerate(nodes):
            if node['node_id'] == nodeid:
                return index

    def plan_clusternode_failover(self, slavenodeid: str, nodes: List[Dict[str, Any]] = None, option: str = 'TAKEOVER',
                                  dryrun: bool = False, deep_copy: bool = False) -> List[Dict[str, Any]]:
        """
        Plan future cluster failover operations and return nodes list that must be after node failover

        :rtype: List[Dict[str, Any]]
        :param slavenodeid: id of slave node
        :param nodes: nodes list
        :param option: TAKEOVER or FORCE
        :param dryrun: do not actually append new plan, just return new nodelist
        :param deep_copy: use deepcopy nodes or not for provided nodes
        :return: renewed nodes list
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)
        elif deep_copy:
            nodes = deepcopy(nodes)

        masternode = self.get_masters(nodes=nodes, slavenodeid=slavenodeid)
        if masternode:
            masternode = masternode
        else:
            raise Exception('Slavenodeid mast be id of slave node, not master')
        masternodeindex = self.get_node_index(nodes=nodes, nodeid=masternode['node_id'])
        slavenodeindex = self.get_node_index(nodes=nodes, nodeid=slavenodeid)
        slavesofmasterreduced = list(
            filter(lambda node: node['node_id'] != slavenodeid, self.get_slaves(nodes=nodes, masternodeid=masternode['node_id'])))

        # swap old-new master-slave fields
        nodes[masternodeindex]['slots'], nodes[slavenodeindex]['slots'] = nodes[slavenodeindex]['slots'], \
            nodes[masternodeindex]['slots']
        nodes[masternodeindex]['master_id'], nodes[slavenodeindex]['master_id'] = slavenodeid, nodes[masternodeindex][
            'master_id']
        nodes[masternodeindex]['flags'], nodes[slavenodeindex]['flags'] = ('slave',), ('master',)
        for node in slavesofmasterreduced:
            nodes[self.get_node_index(node['node_id'])]['master_id'] = slavenodeid

        if not dryrun:
            slave_node = self.get_node(nodes=nodes, nodeid=slavenodeid)
            command = self.create_command('CLUSTER FAILOVER', run_node=slave_node, affected_node=masternode,
                                          command_option=option)
            self.plans.append(command)

        return nodes

    def plan_clusternode_replicate(self, masternodeid: str, slavenodeid: str, nodes: List[Dict[str, Any]] = None,
                                   dryrun: bool = False, deep_copy: bool = False) -> List[Dict[str, Any]]:
        """
        Plan future cluster replication operations and return nodes list that must be after node replication

        :rtype: List[Dict[str, Any]]
        :param masternodeid: id of specified master node
        :param slavenodeid: id of slave node
        :param nodes: nodes list
        :param dryrun: do not actually append new plan, just return new nodelist
        :param deep_copy: use deepcopy nodes or not for provided nodes
        :return: renewed nodes list
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)
        elif deep_copy:
            nodes = deepcopy(nodes)

        slavenode: Dict = self.get_node(nodes=nodes, nodeid=slavenodeid)
        if 'slave' not in slavenode['flags']:
            raise Exception('Slavenodeid must be id of slave node, not master')
        newmasternode: Dict = self.get_node(nodes=nodes, nodeid=masternodeid)
        if 'master' not in newmasternode['flags']:
            raise Exception('Masternodeid must be id of master node, not slave')

        nodes[self.get_node_index(nodes=nodes, nodeid=slavenodeid)]['master_id'] = masternodeid

        if not dryrun:
            command = self.create_command('CLUSTER REPLICATE', run_node=slavenode, affected_node=newmasternode)
            self.plans.append(command)

        return nodes

    def cluster_execute(self, ip: str, port: Union[int, str], command: str) -> bool:
        """
        Executor method

        :param ip: ip address of target execute command host
        :param port: port address of target execute command redis instance
        :param command: string with full command
        :return: True if command was successful else False
        """
        resp = None
        for n in itertools.count(start=1, step=1):
            try:
                redis_node = self.rc.get_node(host=ip, port=port)
                resp = redis_node.redis_connection.execute_command(command).decode('utf-8')
                print(f'Cluster answer: {resp}')
                if resp == "OK":
                    resp = True
                    break
                print(f"Node {ip}:{port} not accept command {command}. Retry {n}/5\nSleep 2m...")
                sleep(120)
                if n > 5:
                    raise Exception(f"Node {ip}:{port} not accept command {command}")
            except Exception as e:
                if n > 5:
                    raise Exception(
                        f'Can not execute command with args: ip = {ip}, port = {port}, command = {command} ')
                print(f"Got exception:\n{e}\nRepeat {n}/5\nSleep 2m...")
                sleep(120)
                break
        return resp

    def cluster_plan_execute(self, plans: list = None, timeout: int = 90) -> bool:
        """
        Execute plan with timeout

        :param plans: list of plan dicts like {}
        {'func': func, 'args': [], 'kwargs': {'kwarg1': value1, 'kwarg1': 'value2', 'msg': 'human like description'}}
        :param timeout: timeout between operations
        :return: bool
        """
        if plans is None:
            plans = self.plans

        for plan in plans:
            print(plan['msg'])
            plan['func'](*plan['args'], **plan['kwargs'])
            sleep(timeout)
        self.currentnodes = self.get_current_nodes()
        return True

    def find_candidate_for_failover(self, masternodeid: str, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Optional[str]:
        """
        Return slavenodeid placed on different server of masternodeid with choose server with the lowest number of masters

        :rtype: Optional[str]
        :param masternodeid: nodeid of master node that become slave
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: situable slavenodeid or None
        """
        if nodes is None:
            nodes = self.currentnodes
        nodesgroup = self.get_nodes_groups(nodes=nodes)

        # find group with the lowest number of masters
        top_masters_groups: Counter = Counter(
            {group: len(self.get_masters(nodes=nodelist)) for group, nodelist in nodesgroup.items()})
        del top_masters_groups[self.get_node_group(nodes=nodes, nodeid=masternodeid)]
        if top_masters_groups:
            # iterate over reversed top (from min master count to max mastercount per group)
            for group, _ in top_masters_groups.most_common()[::-1]:
                # check that group has connected to master slave
                connectedslave = self.get_slaves(nodes=nodesgroup[group], masternodeid=masternodeid, maxport=maxport)
                if connectedslave:
                    return connectedslave.pop()['node_id']
            return None
        else:
            return None

    def print_cluster_info(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, indent: int = 4) -> None:
        """
        Print cluster distribution and skew percentage

        :rtype: None
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param indent: indent for printing
        :return: None
        """
        if nodes is None:
            nodes = self.currentnodes
        masters_group_skew: Dict = self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=-1)
        masters_group_skew_delta = round(max(masters_group_skew.values()) - min(masters_group_skew.values()), 2)
        groupnodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        for group, groups_master_percent in masters_group_skew.items():
            print(f'{" " * indent}Server {group} (has {masters_group_skew[group]}% masters): '
                  f'(masters: {len(self.get_masters(nodes=groupnodes[group], maxport=maxport))!s:3} '
                  f'slaves: {len(self.get_slaves(nodes=groupnodes[group], maxport=maxport))!s:3})')
        print(f'{" " * (indent - 4)}Skew is {masters_group_skew_delta}%\n')

    def get_current_replicas_count(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> int:
        """
        Get current replica count in cluster

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: rounded to min count slaves per node
        """
        if nodes is None:
            nodes = self.currentnodes
        mastercount = len(self.get_masters(nodes=nodes, maxport=maxport))
        if mastercount == 0:
            raise ValueError("Nodes don't have masters at all, can't calculate")
        slavecount = len(self.get_slaves(nodes=nodes, maxport=maxport))
        return int(slavecount / mastercount)

    def get_slaves_counter_of_masters(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Counter:
        """
        Return Counter object like {'masternodeid': slavecount} from nodes list

        :param nodes: nodes list
        :param maxport: maxport counted for slaves
        :return: Counter({'masternodeid': slavecount})
        """
        if nodes is None:
            nodes = self.currentnodes
        masters_slave_counter = Counter()
        for masternode in self.get_masters(nodes=nodes):
            masters_slave_counter[masternode['node_id']] = len(
                self.get_slaves(nodes=nodes, masternodeid=masternode['node_id'], maxport=maxport))
        return masters_slave_counter

    def check_master_does_not_have_desired_replica_count(self, nodes: List[Dict[str, Any]] = None, replicas: int = REPLICAS,
                                                         maxport: int = MAXPORT) -> Dict[str, int]:
        """
        Return dict like {'nodeid': slavecount} if slavecount < replicas param

        :param nodes: nodes list
        :param maxport: maxport counted for slaves
        :param replicas: desired count of replicas
        :return: dict like {'nodeid': slavecount}'
        """
        if nodes is None:
            nodes = self.currentnodes
        return {masternodeid: count for masternodeid, count in
                self.get_slaves_counter_of_masters(nodes=nodes, maxport=maxport).items() if count < replicas}

    def check_master_does_not_have_slaves(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> List[str]:
        """
        Return list with nodeids of masters that don't have slaves

        :param nodes: nodes list
        :param maxport: maxport count
        :return: list of problem masternode ids
        """
        if nodes is None:
            nodes = self.currentnodes
        problem_master_nodes = self.get_node(nodes=nodes, nodeid=list(
            self.check_master_does_not_have_desired_replica_count(nodes=nodes, replicas=1, maxport=maxport).keys()))
        return list(map(lambda masternode: masternode['node_id'], problem_master_nodes))

    def find_slave_candidate_for_master_to_replicate(self, masternodeid: str, nodes: List[Dict[str, Any]] = None,
                                                     maxport: int = MAXPORT, replicas: int = REPLICAS) -> str:
        """
        Return best candidate for replication from masternodeid form another group

        :rtype: str
        :param masternodeid: nodeid of master node that slave will replicate from
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param replicas: desired number of replicas
        :return: slavenodeid that can be replicated from masternodeid
        """
        if nodes is None:
            nodes = self.currentnodes

        master_group = self.get_node_group(nodes=nodes, maxport=maxport, nodeid=masternodeid)
        master_node_slaves = self.get_slaves(nodes=nodes, maxport=maxport, masternodeid=masternodeid)
        master_node_slaves_groups = list(
            set(map(lambda node: self.get_node_group(nodes=nodes, maxport=maxport, node=node), master_node_slaves)))

        # try to return slave with problems
        problem_slaves = self.check_slavesofmaster_in_group(nodes=nodes, replicas=replicas, maxport=maxport)
        if master_group in problem_slaves.keys():
            del problem_slaves[master_group]
        for group, problems in problem_slaves.items():
            for problem in problems:
                for slave_node in problem['slaves']:
                    slave_node_group = self.get_node_group(nodes=nodes, maxport=maxport, node=slave_node)
                    if slave_node_group not in master_node_slaves_groups:
                        return slave_node['node_id']

        # or return slave of master with the highest number of slaves
        nodesgroup = self.get_nodes_groups(nodes=nodes, maxport=maxport)

        # remove masternode group from nodesgroup
        if master_group in nodesgroup.keys():
            del nodesgroup[master_group]

        for masternodeid_with_maximum_slave_count, count in self.get_slaves_counter_of_masters(nodes=nodes,
                                                                                               maxport=maxport).most_common():
            slaves_of_top_master = self.get_slaves(nodes=nodes, maxport=maxport,
                                                   masternodeid=masternodeid_with_maximum_slave_count)
            for slave_node in slaves_of_top_master:
                slave_node_group = self.get_node_group(nodes=nodes, maxport=maxport, node=slave_node)
                if slave_node_group in master_node_slaves_groups:
                    continue
                if self.get_node(nodes=self.mergevalueslists(nodesgroup), nodeid=slave_node['node_id']):
                    return slave_node['node_id']

    def find_candidate_for_slave_to_replicate(self, slavenodeid: str, nodes: List[Dict[str, Any]] = None,
                                              excludegroup: Union[str, list, None] = None,
                                              maxport: int = MAXPORT, replicas: int = REPLICAS) -> Optional[str]:
        """
        Return masternodeid of master candidate for slavenodeid replication

        :rtype: Optional[None]
        :param slavenodeid: nodeid of slave node that slave will replicate from new master
        :param nodes: nodes list
        :param excludegroup: group that shouldn't be offered
        :param maxport: reduce ports to maximum value
        :param replicas: desired number of replicas
        :return: masternodeid if node found, else None
        """
        if nodes is None:
            nodes = self.currentnodes

        # get all groupnodes (do not find master with port > maxport)
        nodesgroup = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        slavenode_group = self.get_node_group(nodes=nodes, maxport=maxport, nodeid=slavenodeid)
        # remove slavenode group from all nodesgroup (we find candidate from another groups)
        if slavenode_group in nodesgroup.keys():
            del nodesgroup[slavenode_group]
        # remove excluded nodes group
        if excludegroup:
            if isinstance(excludegroup, str):
                if excludegroup in nodesgroup.keys():
                    del nodesgroup[excludegroup]
            if isinstance(excludegroup, list):
                for group in excludegroup:
                    if group in nodesgroup.keys():
                        del nodesgroup[group]

        groupreducednodelist = self.mergevalueslists(nodesgroup)

        # find global slave counts for masters
        masterslavecounter = self.get_slaves_counter_of_masters(nodes=nodes)
        # iterate from the lowest slave count
        for masternodeid, count in masterslavecounter.most_common()[::-1]:
            # try to find candidate from reduced nodes list
            if masternodeid in list(map(lambda node: node['node_id'], groupreducednodelist)):
                # don't offer for candidate to replicate current master
                if masternodeid != self.get_node(nodes=nodes, nodeid=slavenodeid)['master_id']:
                    slave_nodes_of_master_nodeid = self.get_slaves(nodes=nodes, masternodeid=masternodeid)
                    # best choice, masternode doesn't have any slaves in slave's node groups
                    if slavenode_group not in self.get_nodes_groups(nodes=slave_nodes_of_master_nodeid).keys():
                        return masternodeid
                    elif len(list(filter(lambda group: group != slavenode_group, self.get_nodes_groups(
                            nodes=slave_nodes_of_master_nodeid).keys()))) >= replicas:
                        return masternodeid
            # master should not have slaves in the same dc
        return None

    def cluster_rebalance_iterate(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> Optional[List[Dict[str, Any]]]:
        """
        Return new skew and new nodes plan or None if rebalance stuck in cycle

        :rtype: Optional[List[Dict[str, Any]]]
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: new nodes plan or None if rebalance stuck in cycle
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)

        master_nodes_counter: Dict[str, int] = cluster.check_group_master_distribution(nodes=nodes, maxport=maxport,
                                                                                       skew=0)
        current_skew: int = max(master_nodes_counter.values()) - min(
            master_nodes_counter.values()) if master_nodes_counter else 0

        cluster_group_master_distribution_problem = self.check_group_master_distribution(nodes=nodes,
                                                                                         maxport=maxport,
                                                                                         skew=0)
        nodesgroup = self.get_nodes_groups(nodes=nodes)
        if cluster_group_master_distribution_problem:
            for group, _ in Counter(cluster_group_master_distribution_problem).most_common():
                for masternode in self.get_masters(nodes=nodesgroup[group]):
                    slavenodeid = self.find_candidate_for_failover(nodes=nodes, maxport=maxport,
                                                                   masternodeid=masternode['node_id'])
                    if slavenodeid:
                        slavenode = self.get_node(nodes=nodes, nodeid=slavenodeid)
                        slavenodeid_already_in_plan = list(filter(lambda x: 'CLUSTER FAILOVER' in x['kwargs']['command']
                                                                            and x['kwargs']['port'] == slavenode['port']
                                                                            and x['kwargs']['ip'] == slavenode['host'],
                                                                  self.plans))
                        new_nodes_plan: list = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid,
                                                                              dryrun=True, deep_copy=True)
                        new_master_nodes_counter: Dict[str, int] = cluster.check_group_master_distribution(
                            nodes=new_nodes_plan,
                            maxport=maxport,
                            skew=0)
                        new_master_nodes_counter_positive: Counter = +Counter(new_master_nodes_counter)
                        new_skew_positive: int = max(new_master_nodes_counter.values()) - min(
                            new_master_nodes_counter_positive.values()) if new_master_nodes_counter_positive else 0
                        if not slavenodeid_already_in_plan and new_skew_positive < current_skew:
                            return self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid, deep_copy=True)
        return None

    def cluster_resolve_master_problem(self, problems: List[str], nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT,
                                       replicas: int = REPLICAS) -> Optional[List[Dict[str, Any]]]:
        if nodes is None:
            nodes = self.currentnodes
        for masternodeid in problems:
            slave_node_for_replicate_candidate = self.find_slave_candidate_for_master_to_replicate(nodes=nodes,
                                                                                                   masternodeid=masternodeid,
                                                                                                   maxport=maxport,
                                                                                                   replicas=replicas)
            return self.plan_clusternode_replicate(nodes=nodes, masternodeid=masternodeid,
                                                   slavenodeid=slave_node_for_replicate_candidate, deep_copy=True)
        return None

    def cluster_resolve_slave_problem(self, problems: Dict[str, List[Dict[str, Union[str, List[Dict[str, Any]]]]]],
                                      nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT, replicas: int = REPLICAS) -> Optional[List[Dict[str, Any]]]:
        """
        Return a non-empty dict (new nodes list) if resolved or None

        :rtype: Optional[List[Dict[str, Any]]]
        :param problems: dict list {'group_with_slaves': [ {'master': masternode, 'slaves': [slavenode1, slavenode2...] } ] }
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param replicas: desired number of replicas
        :return: dict with future nodes list if resolve of problem found or None if you don't have problems
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)
        nodes = deepcopy(nodes)

        for n in itertools.count(start=1, step=1):
            if n > 1000:
                for plan in cluster.plans:
                    print(plan['msg'])
                raise Exception("Can't find candidate for replicate, may be you don't have master in other nodes group")
            for group, group_problems in problems.items():
                for problem in group_problems:
                    for slavenode in problem['slaves']:
                        master_nodeid_for_replicate_of_candidate: str = cluster.find_candidate_for_slave_to_replicate(
                            nodes=nodes, excludegroup=group, slavenodeid=slavenode['node_id'], maxport=maxport,
                            replicas=replicas)
                        if master_nodeid_for_replicate_of_candidate:
                            return cluster.plan_clusternode_replicate(nodes=nodes, slavenodeid=slavenode['node_id'],
                                                                      masternodeid=master_nodeid_for_replicate_of_candidate, deep_copy=True)
            if problems:
                rebalance_iteration = self.cluster_rebalance_iterate(nodes=nodes, maxport=maxport)
                if rebalance_iteration:
                    nodes = rebalance_iteration
            else:
                return None


class RedisClusterToolDatacenter(RedisClusterTool):
    MAXPORT = RedisClusterTool.MAXPORT
    SKEW = RedisClusterTool.SKEW
    GROUPSKEW: ClassVar[int] = 30
    REPLICAS = RedisClusterTool.REPLICAS

    def __init__(self, host: str, port: int, passwd: str, inventory: Inventory, skipconnection: bool = False, onlyconnected: bool = False):
        """
        initial func

        :param host: host to connect to redis cluster
        :param port:  port for connect to redis cluster
        :param inventory: class that contain func get_ip_info and return dict like {ip: {dc: dc_name, fqdn: hostname}}
        :param skipconnection: don't connect to redis server
        :param onlyconnected: not use disconnected node
        """
        self.inventory: Inventory = inventory
        super().__init__(host, port, passwd, skipconnection, onlyconnected)

    def get_current_nodes(self, onlyconnected: bool = False) -> List[Dict[str, Any]]:
        """
        return current cluster nodes configuration from actual cluster

        :param onlyconnected: not use disconnected node
        :rtype: List[Dict[str, Any]]
        :return: list like {'node_id': 'nodeid' 'host': 'hostip', 'port': someport,
         'flags': ('slave',), 'master': 'masternodeid', 'ping-sent': 0, 'pong-recv': 1610468870000,
         'link-state': 'connected', 'slots': [], 'migrations': []}
        """
        prepared_nodes = []
        for host, params in self.rc.cluster_nodes().items():
            params['node_id'] = params['node_id']
            params['master_id'] = params['master_id']
            host, port = host.split(':')
            params['host'], params['port'] = host, int(port)
            prepared_nodes.append(params)
        if onlyconnected:
            return self.merge_server_datacenter(inventory=self.inventory,
                                                nodes=sorted(self.filter_only_connected_nodes(
                                                    nodes=self.filter_without_noaddr_flag_nodes(nodes=prepared_nodes)),
                                                    key=lambda node: (node['host'], node['port'])))
        else:
            return self.merge_server_datacenter(inventory=self.inventory,
                                                nodes=sorted(self.filter_without_noaddr_flag_nodes(nodes=prepared_nodes),
                                                             key=lambda node: (node['host'], node['port'])))

    def merge_server_datacenter(self, inventory: Inventory, nodes: List[Dict[str, Any]] = None) -> list:
        """
        return nodes list merged with inventory datacenter and hostname values

        :rtype: list
        :param inventory: class that contains func get_ip_info that return prepared dict like {ip: ip, dc: dc, fqdn: fqdn}
        :param nodes: nodes list
        :return: merged nodes list with datacenter and hostname
        """
        inventory_nodes = dict(map(lambda ip: (ip, inventory.get_ip_info(ip)), self.get_server_ips(nodes=nodes)))
        for index, node in enumerate(nodes):
            nodes[index]["hostname"] = inventory_nodes[node["host"]]["fqdn"]
            nodes[index]["datacenter"] = inventory_nodes[node["host"]]["dc"]
        return nodes

    def get_nodes_groups(self, nodes: list = None, maxport: int = MAXPORT) -> Dict[str, List[Dict[str, Any]]]:
        """
        return nodes places into host groups

        :rtype: Dict[str, List[Dict[str, Any]]]
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: dict like {'group1': [nodeid1,nodeid2], 'group2': [nodeid3, nodeid4]}
        """
        if nodes is None:
            nodes = self.currentnodes
        nodesgroup: defaultdict = defaultdict(list)
        for node in self.nodes_reduced_max_port(maxport=maxport, nodes=nodes):
            nodesgroup[node['datacenter']].append(node)
        return nodesgroup

    def get_nodes_subgroups(self, nodes: list = None, maxport: int = MAXPORT) -> Dict[str, List[Dict[str, Any]]]:
        """
        return nodes placed into host subgroups

        :rtype: Dict[str, List[Dict[str, Any]]]
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: dict like {'subgroup1': [nodeid1,nodeid2], 'subgroup2': [nodeid3, nodeid4]}
        """
        if nodes is None:
            nodes = self.currentnodes
        nodesgroup: defaultdict = defaultdict(list)
        for node in self.nodes_reduced_max_port(maxport=maxport, nodes=nodes):
            nodesgroup[node['host']].append(node)
        return nodesgroup

    def get_nodes_hosts(self, nodes: list = None) -> List[str]:
        """
        return list of hosts in provided list

        :rtype: list
        :param nodes: nodes list
        :return: [host1, host2, host...]
        """
        if nodes is None:
            nodes = self.currentnodes
        return sorted(list(set(map(lambda node: node['host'], nodes))))

    def get_nodes_by_host(self, nodes: List[Dict[str, Any]] = None, host: str = '') -> List[Dict[str, Any]]:
        """
        return list of hosts in provided list

        :rtype: list
        :param nodes: nodes list
        :param host: host for filter
        :return: [node1, node2, node...]
        """
        if nodes is None:
            nodes = self.currentnodes
        return list(filter(lambda node: node['host'] == host, nodes))

    def check_in_group_master_distribution(self, nodes: list = None, maxport: int = MAXPORT,
                                           groupskew: int = GROUPSKEW) -> Dict[str, Dict[int, int]]:
        """
        Return non-empty dict if max-min masters count in group has diff more than skew percents

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param groupskew: max-min in group percentage difference
        :return: dict like {group: {ip1(node['host']): ip1_masterpercent, ip2(node['host']): ip2_masterpercent}} if group has disbalance more than skew percent
        """
        if nodes is None:
            nodes = self.currentnodes
        distribution_problem: defaultdict = defaultdict(dict)
        for group, groupnodes in self.get_nodes_groups(nodes=nodes, maxport=maxport).items():
            groupips: List[str] = self.get_server_ips(nodes=groupnodes, maxport=maxport)
            if len(groupips) > 1:
                group_master_count = len(self.get_masters(nodes=groupnodes, maxport=maxport))
                master_per_server_count: Counter = Counter(
                    list(map(lambda node: node['host'], self.get_masters(nodes=groupnodes, maxport=maxport))))
                # add zeroes to counter
                for ip in groupips:
                    master_per_server_count[ip] += 0
                master_per_server_percentage: Dict = {
                    host: round((100 / group_master_count) * count, 2) if group_master_count != 0 else 0 for host, count
                    in master_per_server_count.items()}
                percents = self.mergevalueslists(master_per_server_percentage)
                if max(percents) - min(percents) > groupskew:
                    distribution_problem[group] = master_per_server_percentage
        return distribution_problem

    def check_distribution_ok(self, nodes: list = None, maxport: int = MAXPORT, replicas: int = REPLICAS,
                              skew: int = SKEW, groupskew: int = GROUPSKEW) -> int:
        """
        check that redis cluster doesn't have master-slave pair in one group or more than one slave in one group

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param skew: max-min master percentage difference
        :param groupskew: max-min master percentage difference in datacenter
        :param replicas: desired number of replicas
        :return: nagios format, 0 if cluster OK, 1 if WARN (just distribution skew problem), 2 if CRITICAL (serious problem with master-slave distribution)
        """
        if nodes is None:
            nodes = self.currentnodes
        if any([self.check_masterslave_in_group(nodes=nodes, maxport=maxport, replicas=replicas),
                self.check_slavesofmaster_in_group(nodes=nodes, maxport=maxport, replicas=replicas),
                self.check_master_does_not_have_desired_replica_count(nodes=nodes, replicas=replicas),
                self.check_master_does_not_have_slaves(nodes=nodes)]):
            return 2
        if any([self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=skew),
                self.check_in_group_master_distribution(nodes=nodes, maxport=maxport, groupskew=groupskew)]):
            return 1
        return 0

    def print_problems(self, nodes: list = None, maxport: int = MAXPORT, skew: int = SKEW, groupskew: int = GROUPSKEW,
                       replicas: int = REPLICAS) -> None:
        """
        Function only print all problems with current cluster distributioin

        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param skew: max-min master percentage difference
        :param groupskew: max-min master percentage difference in datacenter
        :param replicas: desired number of replicas
        :return: None
        """
        if nodes is None:
            nodes = self.currentnodes

        check_slaveofslave_problem = self.check_slaveofslave(nodes=nodes, maxport=maxport)
        if check_slaveofslave_problem:
            print(f"Cluster has slave of slaves problem ({len(check_slaveofslave_problem)}):")
            for nodeids_pair in check_slaveofslave_problem:
                print(f'Slave node {nodeids_pair[0]} is slave of slave {nodeids_pair[1]}')

        check_masterslave_in_group_problem = self.check_masterslave_in_group(nodes=nodes, maxport=maxport,
                                                                             replicas=replicas)
        if check_masterslave_in_group_problem:
            countproblems = sum(map(lambda problems: len(problems), check_masterslave_in_group_problem.values()))
            print(f'Master-slave distribution problem ({countproblems}):')
            for group, problems in check_masterslave_in_group_problem.items():
                for problem in problems:
                    for slavenode in problem['slaves']:
                        print(f'    Datacenter {group} has master {problem["master_id"]["id"]} '
                              f'{problem["master_id"]["host"]}:{problem["master_id"]["port"]} ({problem["master_id"]["hostname"]}) '
                              f'with slave {slavenode["id"]} {slavenode["host"]}:{slavenode["port"]} ({slavenode["hostname"]}) '
                              f'on same datacenter')
            print()

        check_slavesofmaster_on_group_problem = self.check_slavesofmaster_in_group(nodes=nodes, maxport=maxport,
                                                                                   replicas=replicas)
        if check_slavesofmaster_on_group_problem:
            print(
                f'Too many slaves of one master in group problems ({sum(len(x) for x in check_slavesofmaster_on_group_problem.values())}):')
            for group, problems in check_slavesofmaster_on_group_problem.items():
                for problem in problems:
                    subj = ' and '.join([f'{slavenode["id"]} {slavenode["host"]}:{slavenode["port"]}' for slavenode in
                                         problem['slaves']])
                    print(f'    Datacenter {problem["master_id"]["datacenter"]} has master {problem["master_id"]["id"]} '
                          f'{problem["master_id"]["host"]}:{problem["master_id"]["port"]} ({problem["master_id"]["hostname"]}) '
                          f'with {len(problem["slaves"])} slaves {subj} placed in one datacenter {group}')
            print()

        check_master_does_not_have_desired_replica_count: Dict = self.check_master_does_not_have_desired_replica_count(
            nodes=nodes, maxport=maxport, replicas=replicas)
        if check_master_does_not_have_desired_replica_count:
            print(
                f"Masters don't have desired replica count {replicas} problem ({len(check_master_does_not_have_desired_replica_count)}):")
            for masternodeid, count in check_master_does_not_have_desired_replica_count.items():
                masternode = self.get_node(nodes=nodes, nodeid=masternodeid)
                print(
                    f'    Master node {masternode["id"]} ({masternode["datacenter"]} {masternode["hostname"]}) has {count} replicas')
            print()

        check_master_does_not_have_slaves: List[str] = self.check_master_does_not_have_slaves(nodes=nodes,
                                                                                              maxport=maxport)
        if check_master_does_not_have_slaves:
            print(f"Masters don't have slaves problem ({len(check_master_does_not_have_slaves)}):")
            for masternodeid in check_master_does_not_have_slaves:
                masternode = self.get_node(nodes=nodes, nodeid=masternodeid)
                print(
                    f'    Master node {masternodeid} ({masternode["datacenter"]} {masternode["hostname"]}) has not slaves')
            print()

        masters_group_skew: Dict = self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=skew)
        if masters_group_skew:
            print(f'Groups have master distribution skew more than {skew}% (actual '
                  f'{round(max(masters_group_skew.values()) - min(masters_group_skew.values()), 2)}%): {masters_group_skew}\n')

        masters_in_group_skew: Dict = self.check_in_group_master_distribution(nodes=nodes, maxport=maxport,
                                                                              groupskew=groupskew)
        for group, masterspercentage in masters_in_group_skew.items():
            print(f'Group {group} has servers with distribution skew more than {groupskew}% in group (actual '
                  f'{round(max(masterspercentage.values()) - min(masterspercentage.values()), 2)}%): {masterspercentage}\n')

    def print_cluster_info(self, nodes: list = None, maxport: int = MAXPORT, indent: int = 4) -> None:
        """
        Print cluster distribution and skew percentage

        :rtype: None
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :param indent: indent for printing
        :return: None
        """
        if nodes is None:
            nodes = self.currentnodes
        masters_group_skew: Dict = self.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=-1)
        masters_group_skew_delta = round(max(masters_group_skew.values()) - min(masters_group_skew.values()), 2)
        masters_in_group_skew: Dict = self.check_in_group_master_distribution(nodes=nodes, maxport=maxport,
                                                                              groupskew=-1)
        nodesgroups = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        for group, groups_master_percent in masters_group_skew.items():
            print(f'{" " * indent}Group {group} (has {masters_group_skew[group]}% masters): '
                  f'(masters: {len(self.get_masters(nodes=nodesgroups[group], maxport=maxport))!s:3} '
                  f'slaves: {len(self.get_slaves(nodes=nodesgroups[group], maxport=maxport))!s:3})', end='')

            serversips: list = self.get_server_ips(nodes=nodesgroups[group], maxport=maxport)
            if len(serversips) > 1:
                print()
                for ip in serversips:
                    servernodes = list(filter(lambda node: node['host'] == ip, nodesgroups[group]))
                    hostname = servernodes[0]['hostname']
                    print(
                        f'{" " * (4 + indent)}host {hostname} ({ip}) has {masters_in_group_skew[group].get(ip)}% masters of datacenter: ('
                        f'masters: {len(self.get_masters(nodes=servernodes, maxport=maxport))!s:3}'
                        f'slaves: {len(self.get_slaves(nodes=servernodes, maxport=maxport))!s:3})')
            else:
                print(f' server {nodesgroups[group][0]["hostname"]} ({serversips[0]})')
        print(
            f'{" " * (indent - 4)}Skew: {masters_group_skew_delta}%\nActual replica count {self.get_current_replicas_count(nodes=nodes, maxport=maxport)}\n')

    def find_candidate_for_failover(self, masternodeid: str, nodes: list = None, maxport: int = MAXPORT) -> Optional[str]:
        """
        Return slavenodeid placed on different datacenter of masternodeid with choose server with the lowest number of masters

        :rtype: Optional[str]
        :param masternodeid: nodeid of master node that become slave
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: situable slavenodeid or None
        """
        if nodes is None:
            nodes = self.currentnodes
        nodesgroup = self.get_nodes_groups(nodes=nodes)

        # find group with the lowest number of masters
        top_masters_count_in_groups: Counter = Counter(
            {group: len(self.get_masters(nodes=nodelist)) for group, nodelist in nodesgroup.items()})

        masternode_group = self.get_node_group(nodes=nodes, nodeid=masternodeid)
        if masternode_group in top_masters_count_in_groups:
            del top_masters_count_in_groups[masternode_group]

        if top_masters_count_in_groups:
            # iterate over reversed top (from min master count to max mastercount per group)
            for group, _ in top_masters_count_in_groups.most_common()[::-1]:
                # check that group has connected to master slave
                connectedslave = self.get_slaves(nodes=nodesgroup[group], masternodeid=masternodeid, maxport=maxport)
                if connectedslave:
                    slaveips = self.get_server_ips(nodes=connectedslave)
                    if len(slaveips) > 1:  # if more than one server with situable slave find server with the lowest number of master
                        # get master count for each server
                        top_masters_count_in_group: Counter = Counter({
                            ip: len(list(filter(lambda node: node['host'] == ip,
                                                self.get_masters(nodes=nodesgroup[group], maxport=maxport))))
                            for ip in slaveips
                        })
                        # return first slaveid from server with lowest mastercount
                        for serverip, _ in top_masters_count_in_group.most_common()[::-1]:
                            return list(filter(lambda node: node['host'] == serverip, connectedslave)).pop()['node_id']
                    else:
                        return connectedslave.pop()['node_id']
            return None
        else:
            return None

    def cluster_rebalance_iterate(self, nodes: list = None, skew: int = SKEW, groupskew: int = GROUPSKEW,
                                  maxport: int = MAXPORT) -> Optional[List[Dict[str, Any]]]:
        """
        Return new skew and new nodes plan or None if rebalance stuck in cycle

        :rtype: Optional[list]
        :param nodes: nodes list
        :param skew: max-min master percentage difference
        :param groupskew: max-min master percentage difference in datacenter
        :param maxport: reduce ports to maximum value
        :return: new skew and new nodes plan or None if rebalance stuck in cycle
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)

        nodesgroup = self.get_nodes_groups(nodes=nodes)

        master_nodes_counter = cluster.check_group_master_distribution(nodes=nodes, maxport=maxport, skew=0)
        current_skew = max(master_nodes_counter.values()) - min(
            master_nodes_counter.values()) if master_nodes_counter else 0

        cluster_group_master_distribution_problem = self.check_group_master_distribution(nodes=nodes,
                                                                                         maxport=maxport,
                                                                                         skew=skew)
        if cluster_group_master_distribution_problem:
            for group, _ in Counter(cluster_group_master_distribution_problem).most_common():
                for masternode in self.get_masters(nodes=nodesgroup[group]):
                    slavenodeid = self.find_candidate_for_failover(nodes=nodes, maxport=maxport,
                                                                   masternodeid=masternode['node_id'])
                    if slavenodeid:
                        slavenode = self.get_node(nodes=nodes, nodeid=slavenodeid)
                        slavenodeid_already_in_plan = list(filter(lambda x: 'CLUSTER FAILOVER' in x['kwargs']['command']
                                                                            and x['kwargs']['port'] == slavenode['port']
                                                                            and x['kwargs']['ip'] == slavenode['host'],
                                                                  self.plans))
                        new_nodes_plan = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid,
                                                                        dryrun=True, deep_copy=True)
                        new_master_nodes_counter: Dict[str, int] = cluster.check_group_master_distribution(
                            nodes=new_nodes_plan,
                            maxport=maxport,
                            skew=0)
                        new_master_nodes_counter: Counter = +Counter(new_master_nodes_counter)
                        new_master_nodes_counter_positive: Counter = +Counter(new_master_nodes_counter)
                        new_skew_positive = max(new_master_nodes_counter.values()) - min(
                            new_master_nodes_counter_positive.values()) if new_master_nodes_counter_positive else 0
                        if not slavenodeid_already_in_plan and new_skew_positive < current_skew:
                            return self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid, deep_copy=True)

        cluster_in_group_master_distribution_problem = self.check_in_group_master_distribution(nodes=nodes,
                                                                                               maxport=maxport,
                                                                                               groupskew=groupskew)
        if cluster_in_group_master_distribution_problem:
            for group, node_counters in cluster_in_group_master_distribution_problem.items():
                for node_ip, _ in Counter(node_counters).most_common():
                    # find candidate for node in group with the biggest number of masters
                    for masternode in list(filter(lambda node: node['host'] == node_ip,
                                                  self.get_masters(nodes=nodesgroup[group]))):
                        slavenodeid = self.find_candidate_for_failover(nodes=nodes, maxport=maxport,
                                                                       masternodeid=masternode['node_id'])
                        if slavenodeid:
                            slavenode = self.get_node(nodes=nodes, nodeid=slavenodeid)
                            slavenodeid_already_in_plan = list(
                                filter(lambda x: 'CLUSTER FAILOVER' in x['kwargs']['command']
                                                 and x['kwargs']['port'] == slavenode['port']
                                                 and x['kwargs']['ip'] == slavenode['host'],
                                       self.plans))

                            new_nodes_plan = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid,
                                                                            dryrun=True, deep_copy=True)
                            new_cluster_in_group_master_distribution_problem = self.check_in_group_master_distribution(
                                nodes=new_nodes_plan,
                                maxport=maxport,
                                groupskew=0)
                            old_group_skews = {group: max(skew.values()) - min(skew.values()) for group, skew in
                                               cluster_in_group_master_distribution_problem.items()}
                            new_group_skews = {group: max(skew.values()) - min(skew.values()) for group, skew in
                                               new_cluster_in_group_master_distribution_problem.items()}
                            if not slavenodeid_already_in_plan:
                                for skew_group, old_group_skew in old_group_skews.items():
                                    if old_group_skew > new_group_skews.get(skew_group, 0):
                                        return self.plan_clusternode_failover(nodes=nodes, slavenodeid=slavenodeid, deep_copy=True)

        return None

    def levelout_masters(self, nodes: List[Dict[str, Any]] = None, maxport: int = MAXPORT) -> List[Dict[str, Any]]:
        """
        Levelout masters before rebalancing
        :param nodes: nodes list
        :param maxport: reduce ports to maximum value
        :return: planned nodes
        """
        if nodes is None:
            nodes = deepcopy(self.currentnodes)

        # determine how much masters per group should be
        group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
        groups = sorted(group_nodes.keys())
        masters = self.get_masters(nodes=nodes, maxport=maxport)
        floor, remainder = len(masters) // len(groups), len(masters) % len(groups)
        desired_groups_master_num: OrderedDict[str, Union[OrderedDict[str, int], int]] = OrderedDict(map(lambda group: (group, floor), groups))
        for index in range(remainder):
            desired_groups_master_num[groups[index]] += 1
        desired_nodes_skew = dict()
        for group, number in desired_groups_master_num.items():   # there can't be more masters than nodes
            if number > len(group_nodes[group]):
                desired_nodes_skew[group] = len(group_nodes[group])
        for group, actual_nodes_amount in desired_nodes_skew.items():
            skew = desired_groups_master_num[group] - actual_nodes_amount
            desired_groups_master_num[group] = actual_nodes_amount
            while skew != 0:
                # iterate over sorted by number desired groups len over that not includes skew groups
                for desired_nodes_group in list(filter(lambda group: group not in set(desired_nodes_skew.items()),
                                                       dict(sorted(desired_groups_master_num.items(), key=lambda num: num[1])).keys())):
                    if len(group_nodes[desired_nodes_group]) != desired_groups_master_num[desired_nodes_group]:
                        desired_groups_master_num[desired_nodes_group] += 1
                        skew -= 1
                        if skew == 0:
                            break

        for group, number in desired_groups_master_num.items():   # there can't be more masters than nodes
            hosts_in_group = self.get_nodes_hosts(nodes=group_nodes[group])
            nodes_by_hosts: Dict[str, List[Dict[str, Any]]] = dict(map(lambda host: (host, self.get_nodes_by_host(nodes=group_nodes[group], host=host)), hosts_in_group))
            sub_floor, sub_remainder = number // len(hosts_in_group), number % len(hosts_in_group)
            desired_subgroups_master_num: OrderedDict[str, int] = OrderedDict({subgroup: sub_floor for subgroup in hosts_in_group})
            for index in range(sub_remainder):
                desired_subgroups_master_num[hosts_in_group[index]] += 1
            desired_subnodes_skew = dict()
            for subgroup, subnumber in desired_subgroups_master_num.items():  # there can't be more masters than nodes
                if subnumber > len(nodes_by_hosts[subgroup]):
                    desired_subnodes_skew[subgroup] = len(nodes_by_hosts[subgroup])
            for subgroup, actual_nodes_amount in desired_subnodes_skew.items():
                skew = desired_subgroups_master_num[subgroup] - len(nodes_by_hosts[subgroup])
                desired_subgroups_master_num[subgroup] = len(nodes_by_hosts[subgroup])
                while skew != 0:
                    for desired_nodes_subgroup in list(filter(lambda subgroup: subgroup not in set(desired_subnodes_skew.items()),
                                                       dict(sorted(desired_subgroups_master_num.items(), key=lambda num: num[1])).keys())):
                        if len(nodes_by_hosts[desired_nodes_subgroup]) != desired_subgroups_master_num[desired_nodes_subgroup]:
                            desired_subgroups_master_num[desired_nodes_subgroup] += 1
                            skew -= 1
                            if skew == 0:
                                break
            desired_groups_master_num[group] = desired_subgroups_master_num

        # soft rebalance with failover (using only failover)
        for group in groups:
            neighbor_groups: List[str] = list(filter(lambda gr: gr != group, group_nodes.keys()))
            for subgroup in self.get_nodes_hosts(nodes=group_nodes[group]):
                subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                subgroup_masters = self.get_masters(nodes=subgroup_nodes, maxport=maxport)
                master_skew = desired_groups_master_num[group][subgroup] - len(subgroup_masters)

                if master_skew > 0:  # need to get more masters (too low number of masters)
                    for _ in range(0, master_skew):
                        success = False
                        for neighbor_group in neighbor_groups:
                            for neighbor_subgroup, neighbor_subgroup_nodes in self.get_nodes_subgroups(nodes=group_nodes[neighbor_group], maxport=maxport).items():
                                neighbor_subgroup_masters: List[Dict[str, Any]] = self.get_masters(nodes=neighbor_subgroup_nodes, maxport=maxport)
                                if len(neighbor_subgroup_masters) > desired_groups_master_num[neighbor_group][neighbor_subgroup]:
                                    for slave_node in self.get_slaves(nodes=subgroup_nodes, maxport=maxport):
                                        if self.get_node_group(nodes=nodes,
                                                               node=self.get_masters(nodes=nodes, slavenodeid=slave_node['node_id'], maxport=maxport),
                                                               maxport=maxport) == neighbor_subgroup:  # orphaned nodes can't exist
                                            nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slave_node['node_id'])
                                            group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                            subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                            success = True
                                            break  # stop iterate over current group slaves
                                if success:
                                    break  # stop iterate over available groups
                            if success:
                                break  # stop iterate over available groups
                        if success:
                            continue  # continue if we find appropriate node for failover
                        # if we here - we were failed to find
                        for neighbor_group in neighbor_groups:
                            success = False
                            for neighbor_subgroup in self.get_nodes_hosts(nodes=group_nodes[neighbor_group]):
                                neighbor_subgroup_nodes = self.get_nodes_by_host(nodes=nodes, host=neighbor_subgroup)
                                neighbor_subgroup_masters = self.get_masters(nodes=neighbor_subgroup_nodes, maxport=maxport)
                                if len(neighbor_subgroup_masters) > desired_groups_master_num[neighbor_group][neighbor_subgroup]:
                                    subgroup_slaves = self.get_slaves(nodes=subgroup_nodes, maxport=maxport)
                                    if subgroup_slaves:
                                        nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=neighbor_subgroup_masters[-1]['node_id'], slavenodeid=subgroup_slaves[0]['node_id'])
                                        nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=subgroup_slaves[0]['node_id'])
                                        group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                        subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                        success = True
                                        break
                            if success:
                                break
                        if success:
                            continue  # continue if we find appropriate node for failover

                        # if we here, that means we have more master on group member
                        self_group_neighbors_subgroups: List[str] = list(filter(lambda sb: sb != subgroup, self.get_nodes_hosts(nodes=group_nodes[group])))
                        for self_group_subgroup_neighbor in self_group_neighbors_subgroups:
                            self_group_subgroup_neighbor_nodes = self.get_nodes_by_host(nodes=nodes, host=self_group_subgroup_neighbor)
                            self_group_subgroup_neighbor_masters = self.get_masters(nodes=self_group_subgroup_neighbor_nodes, maxport=maxport)
                            if len(self_group_subgroup_neighbor_masters) > desired_groups_master_num[group][self_group_subgroup_neighbor]:
                                subgroup_slaves = self.get_slaves(nodes=subgroup_nodes, maxport=maxport)
                                if subgroup_slaves:
                                    nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=self_group_subgroup_neighbor_masters[-1]['node_id'], slavenodeid=subgroup_slaves[0]['node_id'])
                                    nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=subgroup_slaves[0]['node_id'])
                                    group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                    subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                    break
                elif master_skew < 0:  # need to reduce masters (too high number of masters)
                    for _ in range(master_skew, 0):
                        success = False
                        for neighbor_group in neighbor_groups:
                            for neighbor_subgroup, neighbor_subgroup_nodes in self.get_nodes_subgroups(nodes=group_nodes[neighbor_group], maxport=maxport).items():
                                neighbor_subgroup_masters = self.get_masters(nodes=neighbor_subgroup_nodes, maxport=maxport)
                                if len(neighbor_subgroup_masters) < desired_groups_master_num[neighbor_group][neighbor_subgroup]:
                                    for slave_node in self.get_slaves(nodes=neighbor_subgroup_nodes, maxport=maxport):
                                        if self.get_masters(nodes=nodes, slavenodeid=slave_node['node_id'], maxport=maxport)['host'] == subgroup:
                                            nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=slave_node['node_id'])
                                            group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                            subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                            subgroup_masters: List[Dict[str, Any]] = self.get_masters(nodes=subgroup_nodes, maxport=maxport)
                                            success = True
                                            break  # stop iterate over current group slaves
                                if success:
                                    break  # stop iterate over available groups
                            if success:
                                continue  # continue if we find appropriate node for failover
                        if success:
                            continue  # continue if we find appropriate node for failover
                        # if we here - we were failed to find, will replicate nodes
                        for neighbor_group in neighbor_groups:
                            success = False
                            for neighbor_subgroup, neighbor_subgroup_nodes in self.get_nodes_subgroups(nodes=group_nodes[neighbor_group], maxport=maxport).items():
                                neighbor_subgroup_masters = self.get_masters(nodes=neighbor_subgroup_nodes, maxport=maxport)
                                if len(neighbor_subgroup_masters) < desired_groups_master_num[neighbor_group][neighbor_subgroup]:
                                    neighbor_subgroup_slaves = self.get_slaves(nodes=neighbor_subgroup_nodes, maxport=maxport)
                                    if neighbor_subgroup_slaves:
                                        nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=subgroup_masters[-1]['node_id'], slavenodeid=neighbor_subgroup_slaves[0]['node_id'])
                                        nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=neighbor_subgroup_slaves[0]['node_id'])
                                        group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                        subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                        subgroup_masters = self.get_masters(nodes=subgroup_nodes, maxport=maxport)
                                        success = True
                                        break
                            if success:
                                break

                        # if we here, that means we have more master on group member
                        self_group_neighbors_subgroups = list(filter(lambda sb: sb != subgroup, self.get_nodes_hosts(nodes=group_nodes[group])))
                        for self_group_subgroup_neighbor in self_group_neighbors_subgroups:
                            self_group_subgroup_neighbor_nodes = self.get_nodes_by_host(nodes=nodes, host=self_group_subgroup_neighbor)
                            self_group_subgroup_neighbor_masters = self.get_masters(nodes=self_group_subgroup_neighbor_nodes, maxport=maxport)
                            if len(self_group_subgroup_neighbor_masters) < desired_groups_master_num[group][self_group_subgroup_neighbor]:
                                self_group_subgroup_neighbor_slaves = self.get_slaves(nodes=self_group_subgroup_neighbor_nodes, maxport=maxport)
                                if self_group_subgroup_neighbor_slaves:
                                    nodes = self.plan_clusternode_replicate(nodes=nodes, masternodeid=subgroup_masters[-1][
                                        'node_id'], slavenodeid=self_group_subgroup_neighbor_slaves[0]['node_id'])
                                    nodes = self.plan_clusternode_failover(nodes=nodes, slavenodeid=self_group_subgroup_neighbor_slaves[0]['node_id'])
                                    group_nodes = self.get_nodes_groups(nodes=nodes, maxport=maxport)
                                    subgroup_nodes = self.get_nodes_by_host(nodes=group_nodes[group], host=subgroup)
                                    subgroup_masters = self.get_masters(nodes=subgroup_nodes, maxport=maxport)
                                    break
                else:  # zero skew
                    continue  # nothing to do

        return nodes

    def create_command(self, command: str, run_node: Dict[str, Any], affected_node: Dict[str, Any], args: Union[tuple, list] = tuple(),
                       command_option: str = "") -> Dict:
        """
        Construct a redis command for clusterexecute function with description

        :param command: command to run on redis
        :param run_node: node where to execute command
        :param affected_node: node that will be affected
        :param args: arguments for clusterexecute func
        :param command_option: optional argument for command such as TAKEOVER / FORCE
        :return: command in {func, args, kwargs, msg} format
        """

        if command == 'CLUSTER REPLICATE':
            exec_command = 'CLUSTER REPLICATE ' + affected_node['node_id']
            command_desc = f'Attach slave  {run_node["id"]} {run_node["host"]}:{run_node["port"]} group ' \
                           f'{self.get_node_group(node=run_node)} to {affected_node["id"]} {affected_node["host"]}:{affected_node["port"]} group {self.get_node_group(node=affected_node)}'

        elif command == 'CLUSTER FAILOVER':
            if command_option == "":
                exec_command = 'CLUSTER FAILOVER'
            else:
                exec_command = 'CLUSTER FAILOVER ' + command_option
            command_desc = f'Failover node {run_node["id"]} {run_node["host"]}:{run_node["port"]} group {self.get_node_group(node=run_node)} ' \
                           f'[old master {affected_node["id"]} {affected_node["host"]}:{affected_node["port"]} group {self.get_node_group(node=affected_node)}]'
        else:
            raise Exception(f"Unknown command for redisclustertool: {command}")

        command = {'func': self.cluster_execute, 'args': args,
                   'kwargs': {'ip': run_node['host'],
                              'port': run_node['port'],
                              'command': exec_command},
                   'msg': command_desc
                   }
        return command


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='redis cluster node print helper')

    connect_group = parser.add_argument_group('connect')
    connect_group.add_argument('--host', '-c', type=str, default='127.0.0.1', help='host to connect for')
    connect_group.add_argument('--port', '-p', type=int, required=False, default=7000, help='port to connect for')
    connect_group.add_argument('--password', type=str, required=False, help='redis password')

    optional_group = parser.add_argument_group('optional')
    optional_group.add_argument('--reduce', '-r', type=int, default=65535, required=False,
                                help='reduce port till number. If cluster has port 7200:7220 and parameter defined to 7210, than '
                                     'program will reduce all port higher than 7210 and levelout masters and replicas on port lower')
    optional_group.add_argument('--replicas', type=int, required=False, help='desired number of replicas')
    optional_group.add_argument('--skew', '-s', type=int, default=15,
                                help='desired master count percentage difference per datacenter')
    optional_group.add_argument('--group-skew', '-g', type=int, default=30,
                                help='desired master count percentage difference per server in datacenter')
    optional_group.add_argument('--timeout', '-t', type=int, default=90, help='timeout between operations')
    optional_group.add_argument('--fix-only', action='store_true', help='Only fix problems, skip rebalance')
    optional_group.add_argument('--force', action='store_true', help='Force rebalance iteration')
    optional_group.add_argument('--alive-only', action='store_true', help='Use only connected nodes', default=False)
    optional_group.add_argument('--credentials', type=str, help='credential config file',
                                default='/etc/redisclustertool/config.cfg')
    optional_group.add_argument('--simple', action='store_true', help='Do not use datacenter detection functionality with inventory', default=False)

    optional_group.add_argument('--noslots_ok', action='store_true', help='Still rebalance despite having '
                                                                          'masters without slots')

    monitoring_group = parser.add_argument_group('monitoring')
    monitoring_group.add_argument('--dry-run', action='store_true', help='Only print current distribution problems')
    monitoring_group.add_argument('--nagios', action='store_true', help='Print short message for nagios short line')

    # Example of inventory group
    # inventory_group = parser.add_argument_group('inventory')
    # inventory_group.add_argument("--inventory-host", type=str, default="somehost", help="Inventory host")

    debug_group = parser.add_argument_group('debug')
    debug_group_mutual = debug_group.add_mutually_exclusive_group()
    debug_group_mutual.add_argument('--save-nodes', type=str, required=False,
                                    help='save original nodes objects in json file')
    debug_group_mutual.add_argument('--load-nodes', type=str, required=False,
                                    help='load original nodes objects from json file')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    redis_password = None
    inventory_helper = None

    if isfile(args.credentials):
        config = configparser.ConfigParser()
        config.read(args.credentials)
        if not args.password:
            redis_password = config.get('default', 'redis_password', fallback=redis_password)
    # args has higher priority
    if args.password:
        redis_password = args.password
    # example
    # inventory_helper = MyInventory(host=args.inventory_host)

    # debug
    if args.save_nodes:
        if args.simple or not inventory_helper:
            cluster = RedisClusterTool(host=args.host, port=args.port, passwd=redis_password)
        else:
            cluster = RedisClusterToolDatacenter(host=args.host, port=args.port, passwd=redis_password, inventory=inventory_helper)
        with open(args.save_nodes, 'w') as f:
            json.dump(cluster.currentnodes, f)
    elif args.load_nodes:
        if args.simple or not inventory_helper:
            cluster = RedisClusterTool(host=args.host, port=args.port, passwd=redis_password,
                                       skipconnection=True)
        else:
            cluster = RedisClusterToolDatacenter(host=args.host, port=args.port, passwd=redis_password, inventory=inventory_helper,
                                                 skipconnection=True)
        with open(args.load_nodes, 'r') as f:
            cluster.currentnodes = json.load(f)
    else:
        if args.simple or not inventory_helper:
            cluster = RedisClusterTool(host=args.host, port=args.port, passwd=redis_password,
                                       onlyconnected=args.alive_only)
        else:
            cluster = RedisClusterToolDatacenter(host=args.host, port=args.port, passwd=redis_password, inventory=inventory_helper,
                                                 onlyconnected=args.alive_only)
    if isinstance(cluster, RedisClusterToolDatacenter):
        skew_params = {'skew': args.skew, 'groupskew': args.group_skew}
    else:
        skew_params = {'skew': args.skew}

    if not args.replicas:
        args.replicas = cluster.get_current_replicas_count()

    if args.nagios and cluster.check_distribution_ok(**skew_params,
                                                     replicas=args.replicas) != 0:  # adapt for nagios
        print(f'Cluster has a problems. Run {__file__}')

    # print cluster info
    print(f'Processing with replica count {args.replicas} and use port {args.port}')
    failed_nodes = cluster.check_failed_nodes()
    if failed_nodes:
        print(f'Cluster has failed status node(s): {failed_nodes}')
        sys.exit(2)
    print('Now cluster has instances per group:')
    cluster.print_cluster_info()

    # check that all masters and slaves will be in different datacenters
    print("\nAnd has problems:")
    cluster.print_problems(**skew_params, replicas=args.replicas)
    if cluster.check_distribution_ok(**skew_params, replicas=args.replicas) == 0:
        print('    None')

    if args.dry_run:
        sys.exit(cluster.check_distribution_ok(**skew_params, replicas=args.replicas))

    if not cluster.check_distribution_possibility(replicas=args.replicas):
        print("Can't place all master-slave groups on different groups")
        sys.exit(1)

    # prepare
    planned_nodes = deepcopy(cluster.currentnodes)

    masters_without_slots = cluster.check_master_without_slots(nodes=planned_nodes)
    if masters_without_slots and not args.noslots_ok:
        print('There are masters without slots, refusing to operate. Add --noslots_ok if you still wish to continue')
        print(masters_without_slots)
        sys.exit(1)

    # reduce slave nodes
    if cluster.get_max_port() > args.reduce:
        for n in itertools.count(start=1, step=1):
            if n > 1000:
                for plan in cluster.plans:
                    print(plan['msg'])
                raise Exception('Too many cycles. Stuck in a cycle during reducing nodes'
                                ' Maybe you need to increase skew parameter')

            master_nodes_for_slave: list = list(
                filter(lambda node: node['port'] > args.reduce, cluster.get_masters(nodes=planned_nodes)))

            if master_nodes_for_slave:
                for masternode in master_nodes_for_slave:
                    slavenodeid = cluster.find_candidate_for_failover(nodes=planned_nodes, maxport=args.reduce,
                                                                      masternodeid=masternode['node_id'])
                    if slavenodeid:
                        planned_nodes = cluster.plan_clusternode_failover(nodes=planned_nodes, slavenodeid=slavenodeid)
                        continue
                    slave_node_for_replicate_candidate = cluster.find_slave_candidate_for_master_to_replicate(
                        nodes=planned_nodes, maxport=args.reduce,
                        masternodeid=masternode['node_id'])
                    planned_nodes = cluster.plan_clusternode_replicate(nodes=planned_nodes, masternodeid=masternode['node_id'],
                                                                       slavenodeid=slave_node_for_replicate_candidate)
                    planned_nodes = cluster.plan_clusternode_failover(nodes=planned_nodes,
                                                                      slavenodeid=slave_node_for_replicate_candidate)
                    break
            else:
                break
    if not args.fix_only:
        distribution_check = cluster.check_distribution_ok(**skew_params, nodes=planned_nodes,
                                                               replicas=args.replicas, maxport=args.reduce)
        if distribution_check != 0 or args.force:
            planned_nodes = cluster.levelout_masters(nodes=planned_nodes, maxport=args.reduce)
            planned_nodes = cluster.levelout_slaves(nodes=planned_nodes, replicas=args.replicas, maxport=args.reduce)
    else:
        # fix problems
        for n in itertools.count(start=1, step=1):
            if n > 1000:
                for plan in cluster.plans:
                    print(plan['msg'])
                raise Exception('Too many cycles. Is it stuck in a cycle? Maybe you need to increase skew parameter')

            master_does_not_have_slaves_resolve = cluster.cluster_resolve_master_problem(
                problems=cluster.check_master_does_not_have_slaves(nodes=planned_nodes, maxport=args.reduce),
                nodes=planned_nodes, maxport=args.reduce, replicas=args.replicas)
            if master_does_not_have_slaves_resolve:
                planned_nodes = master_does_not_have_slaves_resolve
                continue

            masterslave_in_group_resolve = cluster.cluster_resolve_slave_problem(
                problems=cluster.check_masterslave_in_group(nodes=planned_nodes, replicas=args.replicas,
                                                            maxport=args.reduce),
                nodes=planned_nodes, maxport=args.reduce, replicas=args.replicas)
            if masterslave_in_group_resolve:
                planned_nodes = masterslave_in_group_resolve
                continue

            master_does_not_have_desired_replica_count_resolve = cluster.cluster_resolve_master_problem(
                problems=list(
                    cluster.check_master_does_not_have_desired_replica_count(nodes=planned_nodes, replicas=args.replicas,
                                                                             maxport=args.reduce).keys()),
                nodes=planned_nodes, maxport=args.reduce, replicas=args.replicas)
            if master_does_not_have_desired_replica_count_resolve:
                planned_nodes = master_does_not_have_desired_replica_count_resolve
                continue

            slaveofmaster_on_group_resolve = cluster.cluster_resolve_slave_problem(
                problems=cluster.check_slavesofmaster_in_group(nodes=planned_nodes, maxport=args.reduce,
                                                               replicas=args.replicas),
                nodes=planned_nodes, maxport=args.reduce, replicas=args.replicas)
            if slaveofmaster_on_group_resolve:
                planned_nodes = slaveofmaster_on_group_resolve
                continue

            if cluster.check_distribution_ok(**skew_params, nodes=planned_nodes,
                                             replicas=args.replicas, maxport=args.reduce) in (
                    0, 1):  # if OK or WARN (skew check) it's OK
                break
            raise Exception("All problems was resolved, but checks not ok")

    if cluster.plans:
        print('Printing new plan:')
        for plan in cluster.plans:
            print(plan['msg'])
        print()
        print(
            f"It will take {len(cluster.plans)} iterations with timeout {args.timeout} and will take {datetime.timedelta(seconds=args.timeout * len(cluster.plans))} time")

    # print cluster info
    print("\nCluster will have instances per group:")
    cluster.print_cluster_info(nodes=planned_nodes, maxport=args.reduce)
    print("\nAnd will have problems")
    cluster.print_problems(**skew_params, nodes=planned_nodes, replicas=args.replicas,
                           maxport=args.reduce)
    if cluster.check_distribution_ok(nodes=planned_nodes, **skew_params,
                                     replicas=args.replicas, maxport=args.reduce) == 0:
        print('    None')
    print()

    if cluster.plans:
        print(f'Proceed plan to execute with timeout {args.timeout} seconds between operations? y/n')
        while True:
            choice = input().lower()
            if choice in ('yes', 'y', 'ye'):
                print(
                    f"Will be finished at {(datetime.datetime.now() + datetime.timedelta(seconds=args.timeout * len(cluster.plans))).strftime('%Y-%m-%d %H:%M')}")
                cluster.cluster_plan_execute(timeout=args.timeout)
                sys.exit(0)
            elif choice in ('no', 'n'):
                sys.exit(0) if cluster.check_distribution_ok() == 0 else sys.exit(1)
            else:
                print("Please respond with 'yes' or 'no'")
