import salt.client
import logging
import traceback
import sys


logging.basicConfig(level=logging.INFO)

__LOADER__ = ['ja108068.load.dip.sina.com.cn', 'yf140066.scribe.dip.sina.com.cn', 'tc142056.scribe.dip.sina.com.cn', 'bx001121.scribe.dip.sina.com.cn', 'tc142058.scribe.dip.sina.com.cn', 'bx002134.scribe.dip.sina.com.cn',
              'yf140084.scribe.dip.sina.com.cn', 'yf235027.scribe.dip.sina.com.cn', 's052027.tc.ss.dip.sina.com.cn', 's052025.tc.ss.dip.sina.com.cn', 's052026.tc.ss.dip.sina.com.cn', 'bx002133.scribe.dip.sina.com.cn', 'bx001122.scribe.dip.sina.com.cn']

if __name__ == "__main__":
    salt_client = salt.client.LocalClient()

    tgt = __LOADER__
    fun = 'cmd.run'
    arg = [
        'cat /data0/workspace/hadoop_cdh5/conf/server.properties | grep localpath= | awk -F = \'{print $2}\' | awk -F \';\' \'{for (col=1; col<NF; col++) print ($col"/finish/")}\' | xargs -i find {} -type f | xargs rm -rf']
    expr_form = 'list'

    try:
        response = salt_client.cmd(
            tgt=tgt, fun=fun, arg=arg, expr_form=expr_form)
    except Exception:
        logging.error('salt client finish clear error: %s' %
                      traceback.format_exc())

        sys.exit(0)

    for id in response.keys():
        logging.info(id)
        logging.info(response[id])
