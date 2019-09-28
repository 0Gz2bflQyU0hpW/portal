import sys
import logging
import commands

if __name__ == '__main__':
    params = sys.argv
    
    name = params[1]
    queue = params[2]
    user = params[3]
    schedule_time = params[4]
    log_dir = params[5]

    log_file = log_dir + '/python.log'
    
    logging.basicConfig(filename = log_file, level = logging.INFO)

    java = '/usr/local/jdk1.8.0_45/bin/java'
    classpath = '/appparams.jar'
    main = 'com.weibo.dip.scheduler.example.AppParams'

    cmd = '{java} -cp {classpath} {main} {name} {queue} {user} {schedule_time} {log_dir} > {log_dir}/java.log 2>&1'.format(java=java, classpath=classpath, main=main, name=name, queue=queue, user=user, schedule_time=schedule_time, log_dir=log_dir)
    
    logging.info(cmd)

    (status, output) = commands.getstatusoutput(cmd)

    logging.info('status: %s, output: %s', status, output)

    if status != 0:
        raise RuntimeError("The exit code of executing command is not zero!") 
