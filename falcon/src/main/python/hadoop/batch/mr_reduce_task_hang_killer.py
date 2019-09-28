from hadoop import yarn
import redis
import logging
import traceback

logging.basicConfig(level=logging.INFO)

import salt.client
from commons import watchalert
import sys

__DOCKER_NODEMANAGERS__ = ['d216099.eos.dip.sina.com.cn', 'd216100.eos.dip.sina.com.cn', 'd216101.eos.dip.sina.com.cn', 'd216103.eos.dip.sina.com.cn', 'd216104.eos.dip.sina.com.cn', 'd216105.eos.dip.sina.com.cn', 'd216106.eos.dip.sina.com.cn', 'd216107.eos.dip.sina.com.cn', 'd216108.eos.dip.sina.com.cn',
                           'd216109.eos.dip.sina.com.cn', 'd216110.eos.dip.sina.com.cn', 'd216111.eos.dip.sina.com.cn', 'd216112.eos.dip.sina.com.cn', 'd216113.eos.dip.sina.com.cn', 'd216114.eos.dip.sina.com.cn', 'd216115.eos.dip.sina.com.cn', 'd216116.eos.dip.sina.com.cn', 'd216117.eos.dip.sina.com.cn']

if __name__ == "__main__":
    rm1 = 'd056072.eos.dip.sina.com.cn:8088'
    rm2 = 'd056081.eos.dip.sina.com.cn:8088'

    timeout = 5

    redis_host = 'rc7840.eos.grid.sina.com.cn'
    redis_port = 7840
    redis_key_prefix = 'hadoop.yarn.batch.mapreduce.'
    redis_key_expire = 24 * 3600

    yarn_client = yarn.YarnClient(rm1, rm2, timeout)

    parameters = {}

    parameters['states'] = 'RUNNING'
    parameters['applicationTypes'] = 'MAPREDUCE'

    try:
        apps = yarn_client.get_applications(parameters=parameters)
    except Exception:
        logging.error("get failed apps from yarn error: %s" %
                      traceback.format_exc())

        sys.exit(0)

    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0)

    hang_apps = []

    try:
        for app in apps:
            id = app['id']
            progress = app['progress']

            key = redis_key_prefix + id

            value = redis_client.get(name=key)

            if value:
                old_progress = float(value)

                if progress <= old_progress:
                    hang_apps.append(id)

            redis_client.set(name=key, value=str(progress), ex=redis_key_expire)
    except Exception:
        logging.error("get hang apps error: %s" % traceback.format_exc())

        sys.exit(0)

    salt_client = salt.client.LocalClient()

    tgt = __DOCKER_NODEMANAGERS__
    fun = 'cmd.run'
    expr_form = 'list'

    for hang_app in hang_apps:
        command = 'ps aux | grep %s | grep _r_ | awk \'{print $2}\' | xargs kill -9' % hang_app

        logging.info('command: %s' % command)

        try:
            response = salt_client.cmd(
                tgt=tgt, fun=fun, arg=[command], expr_form=expr_form)
        except Exception:
            logging.error('salt client kill hang reduce task error: %s' %
                          traceback.format_exc())

            continue

        for id in response.keys():
            logging.info(id)
            logging.info(response[id])
