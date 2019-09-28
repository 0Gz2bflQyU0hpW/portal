package com.weibo.dip.data.platform.commons.util;

/**
 * Created by yurun on 17/11/13.
 */
public class WatchAlertExample {

    public static void main(String[] args) throws Exception {
        WatchAlert.sendAlarmToUsers("测试报警人服务", "子服务", "标题", "内容", new String[]{"yurun"});

        WatchAlert.sendAlarmToGroups("测试报警组服务", "子服务", "标题", "内容", new String[]{"DIP_ALL"});
    }

}
