package com.weibo.dip.aliyundownload;

import com.weibo.dip.data.platform.redis.RedisClient;

/**
 * @author yurun
 */
public class AliyunKeyClean {

  public static void main(String[] args) {
    String host = "10.13.4.44";
    int port = 6379;

    String key = "aliyun_domain_logs";

    RedisClient client = new RedisClient(host, port);

    client.del(key);

    client.close();
  }

}
