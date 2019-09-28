package com.weibo.dip.data.platform.commons;

/** @author yurun */
public class SinaWatchClientTester {
  public static void main(String[] args) {
    SinaWatchClient client = new SinaWatchClient();

    String service = "测试报警人/组混合报警/报表服务";
    String object = "测试,子服务";
    String subject = "报警标题title";
    String content = "报警内容,\"?&#";

    String[] tos = {"yixuan11", "DIP_TEST"};

    client.alert(service, object, subject, content, tos);
    client.report(service, object, subject, content, tos);
  }
}
