package com.weibo.dip.data.platform.commons.hive;

/** @author yurun */
public class HiveServer2ClientTester {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:hive2://10.77.114.141:10001";

    HiveServer2Client client = new HiveServer2Client(url);

    System.out.println(client.showPartitions("yurun_test", "mobileaction799"));

    client.close();
  }
}
