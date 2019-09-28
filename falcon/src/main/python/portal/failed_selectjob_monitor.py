from commons import timeutil
import MySQLdb
import logging
import traceback
from commons import watchalert

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    times = timeutil.get_last_hour_strtimes()

    sql = '''select
                jobAlias
             from
                selectjobrecord record inner join selectjob job on record.jobId = job.jobId
             where 
                executeTime > "%s"
                and executeTime < "%s"
                and record.state = -1
                and job.jobType != 'manual'
          ''' % (times[0], times[1])

    logging.info("sql: %s" % sql)

    host = 'm7766i.mars.grid.sina.com.cn'
    port = 7766
    user = 'dipadmin6103'
    passwd = '702f23bc282c374'
    db = 'dip_data_analyze'
    timeout = 3

    conn = None

    cursor = None

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user,
                               passwd=passwd, db=db, connect_timeout=timeout)

        cursor = conn.cursor()

        cursor.execute(sql)

        failed_apps = []

        for failed_app in cursor.fetchall():
            failed_apps.append(failed_app[0])

        if failed_apps:
            logging.error("failed apps: %s" % str(failed_apps))

            watchalert.sendAlertToGroups(
                "Portal-SelectJob", "SelectJob Falied", str(len(failed_apps)), str(failed_apps), "DIP_ALL", True, True, False)
    except Exception:
        logging.error("failed apps monitor error: %s" % traceback.format_exc())
    finally:
        if cursor:
            try:
                cursor.close()
            except expression as identifier:
                pass

        if conn:
            try:
                conn.close()
            except expression as identifier:
                pass
