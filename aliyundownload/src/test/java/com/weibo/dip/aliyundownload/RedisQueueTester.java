package com.weibo.dip.aliyundownload;

import com.weibo.dip.data.platform.redis.RedisClient;

/**
 * @author yurun
 */
public class RedisQueueTester {

  public static void main(String[] args) {
    String host = "10.13.4.44";
    int port = 6379;

    String key = "yurun_test_key";

    RedisClient client = new RedisClient(host, port);

    System.out.println(client.lpop(key));

    client.close();
  }

}
