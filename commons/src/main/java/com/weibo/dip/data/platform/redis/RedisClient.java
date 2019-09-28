package com.weibo.dip.data.platform.redis;

import java.util.Objects;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/** Created by yurun on 17/7/11. */
public class RedisClient {

  private JedisPool jedisPool;

  public RedisClient(String host, int port) {
    jedisPool = getJedisPool(host, port);
  }

  private JedisPool getJedisPool(String host, int port) {
    JedisPoolConfig config = new JedisPoolConfig();

    config.setTestOnBorrow(true);

    return new JedisPool(config, host, port);
  }

  /**
   * Get the value of the specified key. If the key does not exist null is returned.
   *
   * @param key key
   * @return value
   */
  public String get(String key) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.get(key);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * The command is exactly equivalent to the following group of commands: set(String, String) +
   * expire(String, int), The operation is atomic.
   *
   * @param key key
   * @param value value
   * @param seconds expire time
   */
  public void set(String key, String value, int seconds) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      jedis.setex(key, seconds, value);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Delete the specified key.
   *
   * @param key key
   * @return reply
   */
  public long del(String key) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.del(key);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Delete the specified keys.
   *
   * @param keys keys
   * @return reply
   */
  public long del(String... keys) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.del(keys);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Add the string value to the head.
   *
   * @param key key
   * @param strings values
   * @return reply
   */
  public long lpush(final String key, final String... strings) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.lpush(key, strings);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Add the string value to the tail.
   *
   * @param key key
   * @param strings values
   * @return reply
   */
  public long rpush(final String key, final String... strings) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.rpush(key, strings);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Return the length of the list stored at the specified key.
   *
   * @param key key
   * @return length
   */
  public long llen(final String key) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.llen(key);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Atomically return and remove the first element of the list.
   *
   * @param key key
   * @return value
   */
  public String lpop(final String key) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.lpop(key);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * SETNX works exactly like set(String, String) with the only difference that if the key already
   * exists no operation is performed. SETNX actually means "SET if Not eXists".
   *
   * @param key key
   * @param value value
   * @return reply, specifically: 1 if the key was set 0 if the key was not set
   */
  public long setnx(final String key, final String value) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.setnx(key, value);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Set a timeout on the specified key.
   *
   * @param key key
   * @param seconds expire time
   * @return reply
   */
  public long expire(final String key, final int seconds) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.expire(key, seconds);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  /**
   * Returns all the keys matching the glob-style pattern as space separated strings.
   *
   * @param pattern pattern
   * @return keys
   */
  public Set<String> keys(String pattern) {
    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();

      return jedis.keys(pattern);
    } finally {
      if (Objects.nonNull(jedis)) {
        jedis.close();
      }
    }
  }

  public void close() {
    jedisPool.close();
  }
}
