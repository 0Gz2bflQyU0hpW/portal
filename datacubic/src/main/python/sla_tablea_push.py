#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urllib2
import json
from datetime import datetime
from datetime import timedelta
import time
import socket
import traceback
import logging

SUMMON_REST = "http://10.13.56.22:8081/rest/summon/v1"

HTTP_TIMEOUT = 30

DATA_SOURCE = "fulllink-total"

SUBTYPES = ["refresh_feed", "play_video"]

PAGE_SIZE = 10000

ACCESS_KEY = "Cu7ysgu1DEE27608C98B"

GRAPHITE_IP = "10.13.80.220"
GRAPHITE_PORT = 2503

ISPS = {
    u"电信": "dianxin",
    u"移动": "yidong",
    u"联通": "liantong",
    u"移通": "yitong",
    u"铁通": "tietong",
    u"教育网": "jiaoyuwang",
    u"广电": "guangdian",
    u"长城宽带": "changchengkuandai",
    u"电信通": "dianxintong",
    u"歌华": "gehua",
    u"华数宽带": "huashukuandai",
    u"方正宽带": "fangzhengkuandai",
    u"宽带通": "kuandaitong",
    u"内网": "neiwang",
    u"油田宽带": "youtiankuandai",
    u"华通宽带": "huatongkuandai",
    u"有线通": "youxiantong",
    u"航数宽网": "hangshukuanwang",
    u"视讯宽带": "shixunkuandai",
    u"蓝波宽带": "lanbokuandai",
    u"E家宽": "ejiakuan",
    u"世纪互联": "shijihulian",
    u"盈联宽带": "yinliankuandai",
    u"中华电信": "zhonghuadianxin",
    u"网通": "wangtong",
    u"赛尔宽带": "saierdaikuan",
    u"网宿": "wangsu",
    u"新世界电讯": "xinshijiedianxun",
    u"视通宽带": "shitongkuandai",
    u"亚太电信": "yataidianxin"
}

TEMPLATE = "fulllink.sla_tablea.byhost.127_0_0_1.%s.%s.%s.%s.%s.%s %s %s\n"

DAY_FORMAT = "%Y%m%d"
UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
INTERVAL_FORMAT_BEGIN = "%Y-%m-%dT16:00:00.000000Z"
INTERVAL_FORMAT_END = "%Y-%m-%dT15:59:59.999999Z"


def get_utc_interval(now):
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)

    begin = now - timedelta(hours=8)
    end = begin + timedelta(hours=23, minutes=59,
                            seconds=59, microseconds=999999)

    return "%s/%s" % (datetime.strftime(begin, UTC_FORMAT), datetime.strftime(end, UTC_FORMAT))


def get_utc_timestamp():
    return datetime.strftime(datetime.utcnow(), UTC_FORMAT)


def get_timestamp():
    return str(int(time.time()))


def main(argv):
    now = datetime.now()

    if argv:
        now = datetime.strptime(argv[0], DAY_FORMAT)

    logging.basicConfig(level=logging.INFO,
                        filename="/var/log/data-platform/sla_tablea_push_%s.log" % datetime.strftime(now, DAY_FORMAT))

    params = {
        "queryType": "search",
        "dataSource": DATA_SOURCE,
        "filter": {
            "fields": [
                {
                    "dimension": "subtype",
                    "values": SUBTYPES
                }
            ]
        },
        "isPaging": True,
        "scrollId": "",
        "size": PAGE_SIZE,
        "order": "desc",
        "interval": get_utc_interval(now),
        "timestamp": get_utc_timestamp(),
        "accessKey": ACCESS_KEY,
        "requestId": get_timestamp()
    }

    total = 0

    try:
        while params.has_key("scrollId"):
            # update timestamp and requestId
            params["timestamp"] = get_utc_timestamp()
            params["requestId"] = get_timestamp()

            url = SUMMON_REST
            headers = {'Content-Type': 'application/json'}
            data = json.dumps(params)

            logging.info("request url: %s, params: %s", url, data)

            request = urllib2.Request(url=url, headers=headers, data=data)

            response = urllib2.urlopen(request, timeout=HTTP_TIMEOUT)

            result = json.load(response)

            if result["code"] == 200:
                data = result["data"]

                if data and data.has_key("hits"):
                    rows = data["hits"]

                    total = total + len(rows)

                    if rows:
                        logging.info("request from summon: %s", str(total))

                        lines = []

                        for row in rows:
                            subtype = row["subtype"] if row[
                                "subtype"] is not None else "NULL"
                            network_type = row["network_type"] if row[
                                "network_type"] is not None else "NULL"
                            app_version = row["app_version"] if row[
                                "app_version"] is not None else "NULL"
                            isp = row["isp"] if row[
                                "isp"] is not None else "NULL"
                            system = row["system"] if row[
                                "system"] is not None else "NULL"

                            app_version = app_version.replace(".", "_")
                            isp = ISPS[isp] if ISPS.has_key(isp) else isp

                            total_num = int(row["total_num"])
                            succeed_num = int(row["succeed_num"])
                            business_error_num = int(row["business_error_num"])

                            timestamp = datetime.strptime(
                                row["timestamp"], UTC_FORMAT) + timedelta(hours=8)

                            lines.append(TEMPLATE % (subtype, network_type, app_version, isp,
                                                     system, "total_num", total_num, int(time.mktime(timestamp.timetuple()))))
                            lines.append(TEMPLATE % (subtype, network_type, app_version, isp,
                                                     system, "succeed_num", succeed_num, int(time.mktime(timestamp.timetuple()))))
                            lines.append(TEMPLATE % (subtype, network_type, app_version, isp,
                                                     system, "business_error_num", business_error_num, int(time.mktime(timestamp.timetuple()))))

                        sender = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            sender.connect((GRAPHITE_IP, GRAPHITE_PORT))

                            for line in lines:
                                try:
                                    sender.send(line)
                                except UnicodeEncodeError as e:
                                    logging.error(
                                        "send line %s error: %s", line, traceback.format_exc())
                        finally:
                            sender.close()

                        params["scrollId"] = data["scroll_id"]
                    else:
                        del params["scrollId"]
                else:
                    logging.warn(result)

                    break
            else:
                logging.warn(result)

                break
        logging.info("total request from summon: %s", str(total))
    except Exception as e:
        logging.error("sla push error: %s", traceback.format_exc())


if __name__ == "__main__":
    main(sys.argv[1:])
