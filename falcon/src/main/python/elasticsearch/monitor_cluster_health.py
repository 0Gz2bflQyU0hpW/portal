import sys
#sys.path.append('')
import yaml
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from elasticsearch import Elasticsearch
import watchalert

config = None
logger = None

class Manager:
    count = 0
    flag = False
    def __init__(self, task):
        self.task = task
        self.client = self.create_elasticsearch_client(task['task']['host']
                                                       , task['task']['port']
                                                       , config['elasticsearch']['manager']['user']
                                                       , config['elasticsearch']['manager']['password'])

    def create_elasticsearch_client(self, host, port, user, password):
        url =  host + ':' + str(port)
        es_client = Elasticsearch(
            hosts=[url],
            maxsize=20,
            http_auth=(user, password),
            sniff_on_start=False,
            use_ssl=False
        )
        return es_client

    def get_cluster_health(self):
        res = self.client.cluster.health()
        logger.debug('cluster health: ' + str(res))
        number_of_nodes = config['elasticsearch']['manager']['number_of_nodes']
        real_number_of_nodes = res['number_of_nodes']
        cluster_status = res['status']
        unassigned_shards = res['unassigned_shards']
        if cluster_status == 'red':
            alert_content = 'The status of cluster is ' + cluster_status + '.'
            logger.debug(alert_content)
            commons.watchalert.sendAlertToGroups("Portal", "Elasticsearch", "[ERROR]The status of cluster is exception." + alert_content, alert_content, "DIP_ALL", True, True, False)
        if unassigned_shards > 0:
            if Manager.count == 3:
                alert_content = 'The unassigned_shards of cluster is ' + str(unassigned_shards) + '.'
                logger.debug(alert_content)
                commons.watchalert.sendAlertToGroups("Portal", "Elasticsearch", "[WARNING]The  cluster is exception." + alert_content, alert_content, "DIP_ALL", True, True, False)
                Manager.flag = True
                Manager.count = 0
            else:
                Manager.count += 1
        else:
            if cluster_status == 'green' and Manager.flag:
                Manager.flag = False
                alert_content = 'The cluster is back to normal and status is green! '
                logger.debug(alert_content)
                commons.watchalert.sendAlertToGroups("Portal", "Elasticsearch", alert_content, alert_content, "DIP_ALL", True, True, False)
            Manager.count = 0

def run(task):
    exec task['task']['code']


def get_logging_level():
    if config['logging']['level'] == 'DEBUG':
        return logging.DEBUG
    if config['logging']['level'] == 'INFO':
        return logging.INFO
    if config['logging']['level'] == 'WARN':
        return logging.WARN
    if config['logging']['level'] == 'ERROR':
        return logging.ERROR
    if config['logging']['level'] == 'FATAL':
        return logging.FATAL


def add_scheduler_tasks(scheduler):
    for task in config['manager']['tasks']:
        interval_unit = task['task']['interval']['units']
        interval_value = int(task['task']['interval']['value'])
        if interval_unit == 'seconds':
            scheduler.add_job(run, 'interval', seconds=interval_value, args=[task])
        elif interval_unit == 'minutes':
            scheduler.add_job(run, 'interval', minutes=interval_value, args=[task])
        elif interval_unit == 'hours':
            scheduler.add_job(run, 'interval', hours=interval_value, args=[task])


def main(argv):
    global config
    global logger
    f = open('../config/manager.yaml', mode='rb')
    config = yaml.load(f)
    f.close()
    logging.basicConfig()
    logger = logging.getLogger(config['logging']['name'])
    logger.setLevel(get_logging_level())
    logging.getLogger('apscheduler.scheduler').setLevel(get_logging_level())

    scheduler = BlockingScheduler()
    scheduler.add_executor(ThreadPoolExecutor(5))
    add_scheduler_tasks(scheduler)
    scheduler.start()

if __name__ == '__main__':
    main(sys.argv)