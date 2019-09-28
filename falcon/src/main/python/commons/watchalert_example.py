#!/usr/bin/env python
# -*- coding:utf-8 -*-
import watchalert

if __name__ == "__main__":
    watchalert.sendAlertToUsers(
        "Adhoc", "Elasticsearch", "测试用户报警", "测试报警请忽略", "yurun", True, True, False)

    watchalert.sendAlertToGroups(
        "Adhoc", "Elasticsearch", "测试用户组报警", "测试报警请忽略", "DIP_TEST", True, True, False)
