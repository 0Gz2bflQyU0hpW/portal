package com.weibo.dip.data.platform.datacubic.streaming.core;

import com.google.common.util.concurrent.AtomicDoubleArray;
import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Summon aggregator in spark streaming.
 *
 * @author yurun
 */
public class SummonAggregator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SummonAggregator.class);

  private static final String BUSINESS = "business";
  private static final String TIMESTAMP = "timestamp";

  private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VALUE_SERIALIZER = "value.serializer";
  private static final String STRING_SERIALIZER =
      "org.apache.kafka.common.serialization.StringSerializer";

  private long interval;
  private int size;
  private String[] servers;
  private String topic;

  private StructField[] fields;

  private int businessIndex;

  private int timestampIndex;

  private int dimensionLength;
  private int[] dimensionIndices;
  private String[] dimensionNames;

  private int metricLength;
  private int[] metricIndices;
  private String[] metricNames;

  private Map<String, Record> records = new HashMap<>();

  private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private Lock readLock = readWriteLock.readLock();
  private Lock writeLock = readWriteLock.writeLock();

  private long lastFlushed = System.currentTimeMillis();

  /**
   * Construct summon aggregator instance.
   *
   * @param interval flush interval
   * @param size flush size
   * @param servers kafka servers
   * @param topic kafka topic
   */
  public SummonAggregator(long interval, int size, String[] servers, String topic) {
    this.interval = interval;
    this.size = size;
    this.servers = servers;
    this.topic = topic;
  }

  private static class Record {

    private String business;
    private long timestamp;
    private String[] dimensions;
    private AtomicDoubleArray metrics;

    public Record(String business, long timestamp, String[] dimensions, int metricLength) {
      this.business = business;
      this.timestamp = timestamp;
      this.dimensions = dimensions;

      metrics = new AtomicDoubleArray(metricLength);
    }

    public String getBusiness() {
      return business;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public String[] getDimensions() {
      return dimensions;
    }

    public AtomicDoubleArray getMetrics() {
      return metrics;
    }

    public void aggregate(double[] seeds) {
      for (int index = 0; index < seeds.length; index++) {
        metrics.getAndAdd(index, seeds[index]);
      }
    }
  }

  private String signature(String business, long timestamp, String[] dimensions) {
    StringBuilder sb = new StringBuilder();

    sb.append(business).append(String.valueOf(timestamp));

    for (String dimension : dimensions) {
      sb.append(dimension);
    }

    return sb.toString();
  }

  /** flush aggregate records to kafka. */
  private void flush() {
    long now = System.currentTimeMillis();

    long aggregateTime = now - lastFlushed;
    long aggregateSize = records.size();

    // judge flush condition: interval or size
    if (aggregateTime >= interval || aggregateSize >= size) {
      writeLock.lock();

      try {
        Map<String, Object> config = new HashMap<>();

        config.put(BOOTSTRAP_SERVERS, String.join(Symbols.COMMA, servers));
        config.put(KEY_SERIALIZER, STRING_SERIALIZER);
        config.put(VALUE_SERIALIZER, STRING_SERIALIZER);

        try (Producer<String, String> producer = new KafkaProducer<>(config)) {
          for (Record record : records.values()) {
            Map<String, Object> datas = new HashMap<>();

            datas.put(BUSINESS, record.getBusiness());

            datas.put(TIMESTAMP, record.getTimestamp());

            for (int index = 0; index < dimensionLength; index++) {
              datas.put(dimensionNames[index], record.getDimensions()[index]);
            }

            for (int index = 0; index < metricLength; index++) {
              datas.put(metricNames[index], record.getMetrics().get(index));
            }

            producer.send(new ProducerRecord<>(topic, GsonUtil.toJson(datas)));
          }
        }

        records.clear();

        long end = System.currentTimeMillis();

        lastFlushed = now;

        LOGGER.info(
            "summon aggregator aggregate time: {}, size: {}, flush time: {}",
            aggregateTime,
            aggregateSize,
            (end - now));
      } catch (Exception e) {
        LOGGER.error("summon aggregator flush error: {}", ExceptionUtils.getStackTrace(e));
      } finally {
        writeLock.unlock();
      }
    }
  }

  /**
   * Aggregate a row.
   *
   * @param row a single row in streaming(spark sql)'s result
   */
  public void aggregate(Row row) {
    // init metadata
    if (Objects.isNull(fields)) {
      writeLock.lock();

      try {
        if (Objects.isNull(fields)) {
          StructField[] structFields = row.schema().fields();

          dimensionLength = 0;
          dimensionIndices = new int[structFields.length];
          dimensionNames = new String[structFields.length];

          metricLength = 0;
          metricIndices = new int[structFields.length];
          metricNames = new String[structFields.length];

          for (int index = 0; index < structFields.length; index++) {
            if (structFields[index].name().equals(BUSINESS)) {
              businessIndex = index;
            } else if (structFields[index].name().equals(TIMESTAMP)) {
              timestampIndex = index;
            } else if (structFields[index].dataType().sameType(DataTypes.StringType)) {
              dimensionIndices[dimensionLength] = index;
              dimensionNames[dimensionLength] = structFields[index].name();
              ++dimensionLength;
            } else {
              metricIndices[metricLength] = index;
              metricNames[metricLength] = structFields[index].name();
              ++metricLength;
            }
          }

          LOGGER.info("business index: {}", businessIndex);
          LOGGER.info("timestamp index: {}", timestampIndex);
          LOGGER.info(
              "dimension length: {}, indices: {}, names: {}",
              dimensionLength,
              dimensionIndices,
              dimensionNames);
          LOGGER.info(
              "metric length: {}, indices: {}, names: {}",
              metricLength,
              metricIndices,
              metricNames);

          fields = structFields;
        }
      } finally {
        writeLock.unlock();
      }
    }

    // flush
    flush();

    // parse
    String business = row.getString(businessIndex);

    long timestamp = row.getLong(timestampIndex);

    String[] dimensions = new String[dimensionLength];
    for (int index = 0; index < dimensionLength; index++) {
      dimensions[index] = row.getString(dimensionIndices[index]);
    }

    double[] metrics = new double[metricLength];
    for (int index = 0; index < metricLength; index++) {
      metrics[index] = Double.valueOf(row.getString(metricIndices[index]));
    }

    String signature = signature(business, timestamp, dimensions);

    // Aggregate
    readLock.lock();

    if (!records.containsKey(signature)) {
      readLock.unlock();

      writeLock.lock();

      try {
        if (!records.containsKey(signature)) {
          records.put(signature, new Record(business, timestamp, dimensions, metricLength));
        }

        readLock.lock();
      } finally {
        writeLock.unlock();
      }
    }

    try {
      records.get(signature).aggregate(metrics);
    } finally {
      readLock.unlock();
    }
  }
}
