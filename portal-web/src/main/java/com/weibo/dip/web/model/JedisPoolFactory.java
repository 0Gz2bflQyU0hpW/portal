package com.weibo.dip.web.model;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by haisen on 2018/5/15.
 */
public class JedisPoolFactory {
  private static JedisPoolConfig config = new JedisPoolConfig();
  private static final JedisPool jedisPool;

  static {
    config.setTestOnBorrow(true);
    jedisPool = new JedisPool(config, "10.23.2.138", 6380, 6000, "dipalarm");
  }

  public static JedisPool creatJedisPool() {
    return jedisPool;
  }
}
