package com.weibo.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.AlarmService;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;

/**
 * Created by yurun on 16/12/21.
 */
public class AlarmClientApplication {

    public static void main(String[] args) {
        AlarmService alarmService = ServiceProxyBuilder.buildLocalhost(AlarmService.class);

        alarmService.sendAlarmToUsers("SinaWatch报警系统", "测试", "用户报警测试", "用户报警内容", new String[]{"yurun"});

        alarmService.sendAlarmToGroups("SinaWatch报警系统", "测试", "用户组报警测试", "用户组报警内容", new String[]{"DIP_ALL"});
    }

}
