#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re


def main(argv):
    source = 0
    process = 0
    sink = 0

    fp = open(argv[0])

    pattern = re.compile('.*RDD source\[(\d+)], process\[(\d+)], sink\[(\d+)]')

    for line in fp:
        match = pattern.match(line)

        if not match:
            continue

        source = source + int(match.group(1))
        process = process + int(match.group(2))
        sink = sink + int(match.group(3))

    print "source: %d, process: %d, sink: %d" % (source, process, sink)

if __name__ == '__main__':
    main(sys.argv[1:])
