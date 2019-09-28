package com.weibo.dip.data.platform.commons.example;

import com.weibo.dip.data.platform.commons.util.WatchAlert;

/**
 * Watch alert example.
 *
 * @author yurun
 */
public class WatchAlertExample {

  public static void main(String[] args) throws Exception {
    WatchAlert.sendAlarmToUsers(
        "1服务 名称（Service）",
        "子服务 名称（SubServcie）",
        "报警标题",
        "报警内容（仅在邮件中显示，私信不显示）",
        new String[] {"weize3"});

    WatchAlert.sendAlarmToGroups(
        "2服务 名称",
        "子服务 名称",
        "报警标题（Subject）",
        "报警内容（Content）（仅在邮件中显示，私信不显示）",
        new String[] {"DIP_STREAMING_TEST"});

    WatchAlert.alert(
        "3报警服务 名称",
        "子服务名称",
        "报警标题（Subject）",
        "报警内容（Content）（仅在邮件中显示，私信不显示）",
        new String[] {"weize3", "DIP_STREAMING_TEST"});

    WatchAlert.report(
        "4报表服务&名称",
        "子服务名称",
        "报警标题（Subject）",
        "报警内容（Content）（仅在邮件中显示，私信不显示）",
        new String[] {"weize3", "DIP_STREAMING_TEST"});
  }
}
