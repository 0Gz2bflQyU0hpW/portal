package com.weibo.dip.ml.godeyes;

import com.weibo.dip.data.platform.kafka.KafkaReader;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import com.weibo.dip.data.platform.redis.RedisClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by yurun on 17/7/11. */
public class GodEyesServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GodEyesServer.class);

  private static final String GC_ZOOKEEPER_PATH =
      "d013004044.hadoop.dip.weibo.com:2181/kafka_intra";
  private static final String GC_TOPIC = "godeyes_collect";
  private static final String GC_CONSUMER_GROUP = "godeyes";
  private static final int GC_THREADS = 1;
  private static final int GC_BUFFER_SIZE = 1000;

  private static final String REDIS_HOST = "10.13.4.44";
  private static final int REDIS_PORT = 6379;

  private static final String GM_KAFKA_SERVERS =
      "first.kafka.dip.weibo.com:9092,"
          + "second.kafka.dip.weibo.com:9092,"
          + "third.kafka.dip.weibo.com:9092,"
          + "fourth.kafka.dip.weibo.com:9092,"
          + "fifth.kafka.dip.weibo.com:9092";
  private static final String GM_TOPIC = "dip-kafka2es-common";

  private static final String GH_KAFKA_SERVERS = "10.13.4.44:9092";
  private static final String GH_TOPIC = "godeyes_history";

  private static final String MODEL_PATH = "/user/godeyes/model";

  private static final int GS_HANDLER_COUNT = 3;

  private static final String UNDERLINE = "_";
  private static final String COLON = ":";

  public static void main(String[] args) {
    KafkaReader collectReader =
        new KafkaReader(GC_ZOOKEEPER_PATH, GC_TOPIC, GC_CONSUMER_GROUP, GC_THREADS, GC_BUFFER_SIZE);

    RedisClient cacheClient = new RedisClient(REDIS_HOST, REDIS_PORT);

    KafkaWriter monitorWriter = new KafkaWriter(GM_KAFKA_SERVERS, GM_TOPIC);

    KafkaWriter historyWriter = new KafkaWriter(GH_KAFKA_SERVERS, GH_TOPIC);

    ExecutorService handlers = Executors.newCachedThreadPool();

    for (int index = 0; index < GS_HANDLER_COUNT; index++) {
      handlers.execute(
          new Handler(
              collectReader, cacheClient, monitorWriter, historyWriter, new Predictor(MODEL_PATH)));
    }

    LOGGER.info("GodEyes Server started");

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  //                  collectReader.close();
                  //                  LOGGER.info("collect reader closed");
                  //
                  //                  handlers.shutdown();
                  //
                  //                  try {
                  //                    while (!handlers.awaitTermination(3, TimeUnit.SECONDS)) {
                  //                      LOGGER.info("godeyes handlers stoping ...");
                  //                    }
                  //
                  //                    LOGGER.info("godeyes handlers stoped");
                  //                  } catch (InterruptedException e) {
                  //                    LOGGER.warn("godeyes hanlers await termination, but
                  // interrupted!");
                  //                  }
                  //
                  //                  cacheClient.close();
                  //                  LOGGER.info("cache client closed");
                  //
                  //                  monitorWriter.close();
                  //                  LOGGER.info("monitor writer closed");
                  //
                  //                  historyWriter.close();
                  //                  LOGGER.info("history writer closed");
                }));
  }
}
