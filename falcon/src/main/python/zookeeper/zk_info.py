#! /usr/bin/env python
"""  Zookeeper Monitor Cluster
    Created by zhiqiang32 18/9/12
"""

import socket
import re
from StringIO import StringIO

class ZooKeeperInfo(object):
    def __init__(self, host='localhost', port='2181', timeout=1):
        self._address = (host, int(port))
        self._timeout = timeout

    def _send_cmd(self, cmd):
        """ Send a 4letter word command to the server """
        s = self._create_socket()
        s.settimeout(self._timeout)
        s.connect(self._address)
        s.send(cmd)
        data = s.recv(2048)
        s.close()
        return data

    def _create_socket(self):
       return socket.socket()

    def get_stats(self):
        """ Get ZooKeeper server stats as a map """
        data = self._send_cmd('mntr')
        if data:
            return self._parse(data, "\t")
        else:
            data = self._send_cmd('stat')
            return self._parse_stat(data)

    def _parse(self, data, split):
        """ Parse the output from the 'mntr' 4letter word command """
        h = StringIO(data)
        result = {}
        for line in h.readlines():
            try:
                key, value = self._parse_line(line, split)
                result[key] = value
            except ValueError:
                pass  # ignore broken lines

        return result

    def _parse_stat(self, data):
        """ Parse the output from the 'stat' 4letter word command """
        h = StringIO(data)
        result = {}
        version = h.readline()
        if version:
            result['zk_version'] = version[version.index(':') + 1:].strip()
        # skip all lines until we find the empty one
        while h.readline().strip(): pass
        for line in h.readlines():
            m = re.match('Latency min/avg/max: (\d+)/(\d+)/(\d+)', line)
            if m is not None:
                result['zk_min_latency'] = int(m.group(1))
                result['zk_avg_latency'] = int(m.group(2))
                result['zk_max_latency'] = int(m.group(3))
                continue
            m = re.match('Received: (\d+)', line)
            if m is not None:
                result['zk_packets_received'] = int(m.group(1))
                continue
            m = re.match('Sent: (\d+)', line)
            if m is not None:
                result['zk_packets_sent'] = int(m.group(1))
                continue
            m = re.match('Outstanding: (\d+)', line)
            if m is not None:
                result['zk_outstanding_requests'] = int(m.group(1))
                continue
            m = re.match('Mode: (.*)', line)
            if m is not None:
                result['zk_server_state'] = m.group(1)
                continue
            m = re.match('Node count: (\d+)', line)
            if m is not None:
                result['zk_znode_count'] = int(m.group(1))
                continue

        return result


    def _parse_line(self, line, split):
        try:
            key, value = map(str.strip, line.split(split))
        except ValueError:
            raise ValueError('Found invalid line: %s' % line)

        if not key:
            raise ValueError('The key is mandatory and should not be empty')

        try:
            value = int(value)
        except (TypeError, ValueError):
            pass

        return key, value
