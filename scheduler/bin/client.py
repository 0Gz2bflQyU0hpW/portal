#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import commands

user = 'root'

java_home = '/usr/java/default'

classpaths = [
              '/data0/workspace/scheduler/scheduler/conf/',
              '/data0/workspace/scheduler/scheduler/target/scheduler-2.0.0.jar',
              '/data0/workspace/scheduler/scheduler/target/scheduler-2.0.0-lib/*']

main_class = 'com.weibo.dip.scheduler.client.ConsoleSchedulerClient'

if __name__ == '__main__':
    command = 'sudo -u %s %s/bin/java -cp %s %s %s' % (user, java_home, ':'.join(classpaths), main_class, ' '.join(sys.argv[1:]))

    print commands.getoutput(command)