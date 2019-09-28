#!/usr/bin/python
# -r- coding: UTF-8 -*-

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s', filename='/var/log/falcon/resolve-inode-full.log', filemode='a')
import salt.client
import traceback
from commons import watchalert
import re


'''
author: jianhong1
date: 2017-09-08
'''

hadoop_group = [
    's004238.bx.ss.dip.sina.com.cn',
    's004218.bx.ss.dip.sina.com.cn',
    's004217.bx.ss.dip.sina.com.cn',
    'd057206.eos.dip.sina.com.cn',
    'd057203.eos.dip.sina.com.cn',
    'd056163.eos.dip.sina.com.cn',
    'd056162.eos.dip.sina.com.cn',
    'd056161.eos.dip.sina.com.cn',
    'd056159.eos.dip.sina.com.cn',
    'd056158.eos.dip.sina.com.cn',
    'd216117.eos.dip.sina.com.cn',
    'd056021.eos.dip.sina.com.cn',
    'd056023.eos.dip.sina.com.cn',
    'd216116.eos.dip.sina.com.cn',
    'd216115.eos.dip.sina.com.cn',
    'd216114.eos.dip.sina.com.cn',
    'd216113.eos.dip.sina.com.cn',
    'd216112.eos.dip.sina.com.cn',
    'd216111.eos.dip.sina.com.cn',
    'd216110.eos.dip.sina.com.cn',
    'd216109.eos.dip.sina.com.cn',
    'd216108.eos.dip.sina.com.cn',
    'd216107.eos.dip.sina.com.cn',
    'd216106.eos.dip.sina.com.cn',
    'd216105.eos.dip.sina.com.cn',
    'd216104.eos.dip.sina.com.cn',
    'd216103.eos.dip.sina.com.cn',
    'd216101.eos.dip.sina.com.cn',
    'd216100.eos.dip.sina.com.cn',
    'd216099.eos.dip.sina.com.cn',
    'd056157.eos.dip.sina.com.cn',
    'd056156.eos.dip.sina.com.cn',
    'd056155.eos.dip.sina.com.cn',
    'd056154.eos.dip.sina.com.cn',
    'd056153.eos.dip.sina.com.cn',
    'd056152.eos.dip.sina.com.cn',
    'd056151.eos.dip.sina.com.cn',
    'd056150.eos.dip.sina.com.cn',
    'd056149.eos.dip.sina.com.cn',
    'd056148.eos.dip.sina.com.cn',
    'd056147.eos.dip.sina.com.cn',
    'd056146.eos.dip.sina.com.cn',
    'd056125.eos.dip.sina.com.cn',
    'd056127.eos.dip.sina.com.cn',
    'd056128.eos.dip.sina.com.cn',
    'd056129.eos.dip.sina.com.cn',
    'd056130.eos.dip.sina.com.cn',
    'd056131.eos.dip.sina.com.cn',
    'd056132.eos.dip.sina.com.cn',
    'd056133.eos.dip.sina.com.cn',
    'd056134.eos.dip.sina.com.cn',
    'd056135.eos.dip.sina.com.cn',
    'd056136.eos.dip.sina.com.cn',
    'd056137.eos.dip.sina.com.cn',
    'd056138.eos.dip.sina.com.cn',
    'd056139.eos.dip.sina.com.cn',
    'd056140.eos.dip.sina.com.cn',
    'd056141.eos.dip.sina.com.cn',
    'd056142.eos.dip.sina.com.cn',
    'd056143.eos.dip.sina.com.cn',
    'd056144.eos.dip.sina.com.cn',
    'd056145.eos.dip.sina.com.cn',
    'd056124.eos.dip.sina.com.cn',
    'd056123.eos.dip.sina.com.cn',
    'd056122.eos.dip.sina.com.cn',
    'd056121.eos.dip.sina.com.cn',
    'd056120.eos.dip.sina.com.cn',
    'd056119.eos.dip.sina.com.cn',
    'd056118.eos.dip.sina.com.cn',
    'd056117.eos.dip.sina.com.cn',
    'd056116.eos.dip.sina.com.cn',
    'd056114.eos.dip.sina.com.cn',
    'd056113.eos.dip.sina.com.cn',
    'd056112.eos.dip.sina.com.cn',
    'd056111.eos.dip.sina.com.cn',
    'd056110.eos.dip.sina.com.cn',
    'd056109.eos.dip.sina.com.cn',
    'd056108.eos.dip.sina.com.cn',
    'd056107.eos.dip.sina.com.cn',
    'd056106.eos.dip.sina.com.cn',
    'd056105.eos.dip.sina.com.cn',
    'd056104.eos.dip.sina.com.cn',
    'd056103.eos.dip.sina.com.cn',
    'd056102.eos.dip.sina.com.cn',
    'd056101.eos.dip.sina.com.cn',
    'd056099.eos.dip.sina.com.cn',
    'd056098.eos.dip.sina.com.cn',
    'd056096.eos.dip.sina.com.cn',
    'd056095.eos.dip.sina.com.cn',
    'd056094.eos.dip.sina.com.cn',
    'd056093.eos.dip.sina.com.cn',
    'd056092.eos.dip.sina.com.cn',
    'd056091.eos.dip.sina.com.cn',
    'd056090.eos.dip.sina.com.cn',
    'd056087.eos.dip.sina.com.cn',
    'd056085.eos.dip.sina.com.cn',
    'd056082.eos.dip.sina.com.cn',
    'd056081.eos.dip.sina.com.cn',
    'd056080.eos.dip.sina.com.cn',
    'd056079.eos.dip.sina.com.cn',
    'd056076.eos.dip.sina.com.cn',
    'd056073.eos.dip.sina.com.cn',
    'd056072.eos.dip.sina.com.cn',
    'd056057.eos.dip.sina.com.cn',
    'd056051.eos.dip.sina.com.cn',
    'd056046.eos.dip.sina.com.cn',
    'd056044.eos.dip.sina.com.cn',
    'd056043.eos.dip.sina.com.cn',
    'd056039.eos.dip.sina.com.cn',
    'd056038.eos.dip.sina.com.cn',
    'd056033.eos.dip.sina.com.cn',
    'd056029.eos.dip.sina.com.cn',
    '77-109-206-bx-core.jpool.sinaimg.cn',
    '77-109-205-bx-core.jpool.sinaimg.cn',
    '77-109-203-bx-core.jpool.sinaimg.cn',
    '77-109-201-bx-core.jpool.sinaimg.cn',
    '77-109-200-bx-core.jpool.sinaimg.cn',
    '77-109-199-bx-core.jpool.sinaimg.cn',
    '77-109-198-bx-core.jpool.sinaimg.cn',
    '77-109-197-bx-core.jpool.sinaimg.cn',
    's004244.bx.ss.dip.sina.com.cn']


THRESHOLD = 50


def main():
    salt_client = salt.client.LocalClient()
    for hadoop_node in hadoop_group:

        '''节点无响应抛出异常'''
        try:
            disk_inode = salt_client.cmd(hadoop_node, 'cmd.run', ['df -i /var'])
            disk_inode_value = re.split('\s+', disk_inode[hadoop_node])[-2].strip('%')
        except:
            content = "HadoopNode no response, HadoopNode: %s\n %s" % (hadoop_node, traceback.format_exc())
            logging.error(content)
            continue

        logging.info('HadoopNode: %s, the inode of /var: %s%%' % (hadoop_node, disk_inode_value))
        if int(disk_inode_value) < THRESHOLD:
            continue

        '''执行删除操作'''
        salt_client.cmd(hadoop_node, 'cmd.run', ['find /var/spool/clientmqueue/ -type f | xargs rm –f'])
        salt_client.cmd(hadoop_node, 'cmd.run', ['find /var/log/hadoop-mapreduce/ -type f | xargs rm –f'])

        disk_inode = salt_client.cmd(hadoop_node, 'cmd.run', ['df -i /var'])
        disk_inode_value = re.split('\s+', disk_inode[hadoop_node])[-2].strip('%')
        content = 'HadoopNode: %s, after rm, the inode of /var: %s%%' % (hadoop_node, disk_inode_value)
        logging.info(content)


if __name__ == "__main__":
    main()

