package com.weibo.dip.cdn.util;

import org.apache.spark.streaming.StreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Objects;

/**
 * Created by yurun on 17/12/4.
 */
public class DipStreaming {

    private static final Logger LOGGER = LoggerFactory.getLogger(DipStreaming.class);

    public static final String REDIS_HOST = "rc7840.eos.grid.sina.com.cn";
    public static final int REDIS_PORT = 7840;

    public static final String SPARK_STREAMING = "spark_streaming";

    public static final String SPARK_APP_NAME = "spark.app.name";

    public static final String STARTING = "starting";
    public static final String STARTED = "started";

    public static final String STOPING = "stoping";
    public static final String STOPED = "stoped";

    public static final long WAITER_SLEEP = 3000;

    public static void start(StreamingContext context) {
        String appName = context.conf().get(SPARK_APP_NAME);

        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);

        try {
            jedis.hset(SPARK_STREAMING, appName, STARTING);
            context.start();
            jedis.hset(SPARK_STREAMING, appName, STARTED);

            LOGGER.info("streaming {} app started", appName);

            Thread waiter = new Thread(() -> {
                while (!jedis.hget(SPARK_STREAMING, appName).equals(STOPING)) {
                    try {
                        Thread.sleep(WAITER_SLEEP);
                    } catch (InterruptedException e) {
                        LOGGER.warn("waiter sleeped, but interrupt");

                        break;
                    }
                }

                context.stop(false, true);
            });

            waiter.start();
            LOGGER.info("streaming {} waiter started", appName);

            context.awaitTermination();

            jedis.hset(SPARK_STREAMING, appName, STOPED);
            LOGGER.info("streaming {} app, waiter, context stoped", appName);

            try {
                Thread.sleep(WAITER_SLEEP * 3);
            } catch (InterruptedException e) {

            }

            context.stop(true);
            LOGGER.info("spark context stoped");
        } finally {
            if (Objects.nonNull(jedis)) {
                jedis.close();
            }
        }
    }

    public static boolean isStarted(String appName) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);

        try {
            return jedis.hget(SPARK_STREAMING, appName).equals(STARTED);
        } finally {
            if (Objects.nonNull(jedis)) {
                jedis.close();
            }
        }
    }

    public static boolean isStoped(String appName) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);

        try {
            return jedis.hget(SPARK_STREAMING, appName).equals(STOPED);
        } finally {
            if (Objects.nonNull(jedis)) {
                jedis.close();
            }
        }
    }

    public static void stop(String appName) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);

        try {
            jedis.hset(SPARK_STREAMING, appName, STOPING);
        } finally {
            if (Objects.nonNull(jedis)) {
                jedis.close();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(isStarted("StreamingDemo"));
        System.out.println(isStoped("StreamingDemo"));

        stop("StreamingDemo");
    }

}