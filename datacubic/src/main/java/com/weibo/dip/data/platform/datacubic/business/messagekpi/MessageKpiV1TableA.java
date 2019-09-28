package com.weibo.dip.data.platform.datacubic.business.messagekpi;

import com.weibo.dip.data.platform.datacubic.business.Ideo_Download_Base2slow;
import com.weibo.dip.data.platform.datacubic.business.messagekpi.util.MessageKpiCommon;
import com.weibo.dip.data.platform.datacubic.streaming.udf.IpToLocation;
import com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink.ParseUAInfo;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http 和 短连 . Created by liyang28 on 2018/6/12.
 */
public class MessageKpiV1TableA {

  private static final Logger LOGGER = LoggerFactory.getLogger(MessageKpiV1TableA.class);
  private static final FastDateFormat YYYY_MM_DD = FastDateFormat.getInstance("yyyy_MM_dd");

  /**
   * 获得输入的路径 .
   *
   * @param date .
   * @return .
   */
  public static String getInputPath(String date) {
    return "/user/hdfs/rawlog/mweibo_client_messagelog/" + date + "/*";
  }

  /**
   * 获得昨天的格式：yyyy_mm_dd .
   */
  private static String getYesterday() {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -1);
    Date time = cal.getTime();
    return YYYY_MM_DD.format(time);
  }

  /**
   * 获取 sql .
   *
   * @return .
   * @throws Exception .
   */
  private static String getSql() throws Exception {
    StringBuilder sql = new StringBuilder();
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(
          new InputStreamReader(
              Ideo_Download_Base2slow.class.getClassLoader()
                  .getResourceAsStream(MessageKpiCommon.V1_TABLE_A),
              CharEncoding.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        sql.append(line);
        sql.append("\n");
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return sql.toString();
  }

  /**
   * main .
   *
   * @param args .
   */
  public static void main(String[] args) {
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);
    SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();
    try {
      session.udf().register(MessageKpiCommon.PARSE_UA_INFO, new ParseUAInfo(),
          DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
      session.udf().register(MessageKpiCommon.IP_TO_LOCATION, new IpToLocation(),
          DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
      String[] fieldNames =
          {"action", "requestStatus", "name", "proto_type", "type", "class", "start", "endTime",
              "ap", "ua", "cip", "isSampled", "ssc", "dl", "ws", "rb", "sc", "sr", "from"};
      StructType schema = MessageKpiCommon.getSchema(fieldNames);
      String date = args.length == 0 ? getYesterday() : args[0];
      LongAccumulator notMatchCount = sc.sc().longAccumulator(MessageKpiCommon.NOT_MATCH_COUNT);
      JavaRDD<Row> rowRdd = MessageKpiCommon
          .getJsonDataRdd(sc, getInputPath(date), fieldNames, notMatchCount);
      Dataset<Row> sourceDs = session.createDataFrame(rowRdd, schema);
      sourceDs.createOrReplaceTempView(MessageKpiCommon.SPARK_CACHE_TABLE);
      Dataset<Row> resultDs = session.sql(getSql());
      LongAccumulator outputCount = sc.sc().longAccumulator(MessageKpiCommon.OUTPUT_COUNT);
      MessageKpiCommon
          .sendKafka(resultDs, date, outputCount, MessageKpiCommon.V1_TABLE_A_BUSINESS);
      LOGGER.info("output count: {}, not match count: {}", outputCount.value(), notMatchCount
          .value());
    } catch (Exception e) {
      LOGGER.error("error: {}", ExceptionUtils.getFullStackTrace(e));
    } finally {
      sc.stop();
    }
  }

}
