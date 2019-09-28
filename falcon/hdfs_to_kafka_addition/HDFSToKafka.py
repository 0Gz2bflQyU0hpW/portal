import os
import logging
import sys
from time import sleep
import commands
from Util import Properties

VERSION = "2.0.0-SNAPSHOT"
FALCON_HOME = "/data0/workspace/portal"

PROPERTIES_FILE= "hdfs_to_kafka.properties"
STOP_FILE_PATH = Properties(PROPERTIES_FILE).getProperties()['stop.file.path']

LOG_PATH_FILE = "log4j.properties"
LOG_FILE_PATH = Properties(LOG_PATH_FILE).getProperties()['log4j.appender.Falcon.File']



def start(action):
    # if status() == "running":
    #     print "services is running: %s" % getPID()
    #     return

    logging.info("start... the program")
    command_create = "mkdir -p  /var/log/data-platform"
    os.system(command_create)
    command_touch = "touch " + LOG_FILE_PATH
    os.system(command_touch)
    command_changeOwn = "chown hdfs " + LOG_FILE_PATH
    os.system(command_changeOwn)


    command = "sudo -u hdfs java -cp ./:../target/data-platform-falcon-2.0.0-SNAPSHOT.jar:../target/data-platform-falcon-2.0.0-SNAPSHOT-lib/* com.weibo.dip.data.platform.falcon.transport.HDFSToKafkaMain "+action
    result = os.system(command)
    if result == 0:
        logging.info("start... the program OK!")
    else:
        logging.info("start... the program Failed!")

def stop():
    logging.info("stop the program")
    logging.info("add the stop file...")
    command = "touch " + STOP_FILE_PATH
    os.system(command)
    sleep(10)
    logging.info("remove the " + STOP_FILE_PATH + " if exist")
    command = "rm -f " + STOP_FILE_PATH
    result = os.system(command)
    if result == 0:
        logging.info("clean "+STOP_FILE_PATH+" successful")

def usage():
    print "usage:\n python HDFSToKafka.py (arg:addition,produce) :first start the Java Program\n python HDFSToKafka.py recover :recover from the failure\n python HDFSToKafka.py stop :stop the Java Program \n python HDFSToKafka.py status :show the program's status"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    if sys.argv[1] == "addition":
        start(sys.argv[1])
    elif sys.argv[1] == "produce":
        start(sys.argv[1])
    # elif sys.argv[1] == "recover":
    #         recover()
    # elif sys.argv[1] == "status":
    #     status()
    elif sys.argv[1] == "stop":
        stop()
    else:
        usage()
