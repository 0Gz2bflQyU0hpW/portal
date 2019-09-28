package com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil.GsonType;
import com.weibo.dip.data.platform.commons.util.WatchAlert;
import com.weibo.dip.data.platform.datacubic.business.summonagg.common.HdfsUtils;
import com.weibo.dip.data.platform.datacubic.business.summonagg.common.SqlUtils;
import com.weibo.dip.data.platform.datacubic.business.summonagg.common.SummonApi;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.SparkSqlService;

import com.weibo.dip.data.platform.datacubic.business.summonagg.udf.TimeStampDayTransform;
import com.weibo.dip.data.platform.datacubic.business.summonagg.udf.TimeStampHourTransform;
import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

/**
 * Created by liyang28 on 2018/7/27.
 */
public class SparkSqlServiceImpl implements SparkSqlService, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SparkSqlServiceImpl.class);
  private static final Long currentTimeMillis = System.currentTimeMillis();
  private List<String> indexFields = new ArrayList<>();
  private Map<String, String> indexFieldsTypes = new HashMap<>();
  private List<String> indexCastFieldsType = new ArrayList<>();
  private List<String> conditionFields = new ArrayList<>();
  private List<String> allFields = new ArrayList<>();
  private String aggType;
  private String business;
  private String businessAgg;
  private String inputPath;
  private String outputPath;
  private String indexYesterday;
  private String[] fieldNames;
  private LongAccumulator notMatchCount;
  private LongAccumulator matchCount;
  private Dataset<Row> resultDs;
  private JavaRDD<String> lineRdd;


  /**
   * 初始化参数 .
   *
   * @param args .
   */
  public SparkSqlServiceImpl(String[] args) {
    String[] params = getArgs(args);
    business = params[0];
    aggType = params[1];
    indexYesterday = HdfsUtils.getIndexYesterday();
    inputPath = HdfsUtils.getInputPath(business);
    getBusinessAgg();
    outputPath = HdfsUtils.getOutputPath(businessAgg);
  }

  /**
   * 获得参数和聚合类型.
   */
  private String[] getArgs(String[] args) {
    return args[0].split(AggConstants.SPLIT_ARGS);
  }

  /**
   * 获得聚合后的索引.
   */
  private void getBusinessAgg() {
    String[] splitBusinessAggBefore = business.split(AggConstants.AGGREGATION);
    switch (aggType) {
      case AggConstants.DAILY: {
        //按天聚合的索引
        if (splitBusinessAggBefore.length == 1) {
          this.businessAgg = splitBusinessAggBefore[0] + AggConstants.DAILY_AGGREAGTION;
        } else {
          this.businessAgg = splitBusinessAggBefore[0] + AggConstants.DAILY_AGGREAGTION
              + splitBusinessAggBefore[1];
        }

      }
      break;
      case AggConstants.HOUR: {
        //按小时聚合的索引
        if (splitBusinessAggBefore.length == 1) {
          this.businessAgg = splitBusinessAggBefore[0] + AggConstants.HOUR_AGGREAGTION;
        } else {
          this.businessAgg = splitBusinessAggBefore[0] + AggConstants.HOUR_AGGREAGTION
              + splitBusinessAggBefore[1];
        }
      }
      break;
      default:
        LOGGER.error("agg type  error");
        break;
    }
  }

  /**
   * 获得通用的sql .
   */
  private String getCommonSql() {
    try {
      Map<String, Object> mapResult = GsonUtil
          .fromJson(SummonApi.getSummonRespResult(business, currentTimeMillis),
              GsonType.OBJECT_MAP_TYPE);
      Map<String, Object> data = GsonUtil
          .fromJson(mapResult.get(AggConstants.SUMMON_DATA).toString(),
              GsonType.OBJECT_MAP_TYPE);
      List<Map<String, String>> fields = GsonUtil
          .fromJson(data.get(business).toString(), GsonType.OBJECT_LIST_TYPE);
      for (Map<String, String> next : fields) {
        String type = next.get(AggConstants.SUMMON_TYPE);
        String field = next.get(AggConstants.SUMMON_FIELD);
        if (type.equals(AggConstants.SUMMON_STRING)) {
          conditionFields.add(field);
        } else if (type.equals(AggConstants.SUMMON_LONG) || type
            .equals(AggConstants.SUMMON_FLOAT)) {
          if (type.equals(AggConstants.SUMMON_FLOAT)) {
            indexFieldsTypes.put(field, AggConstants.CAST_DECIMAL);
          } else {
            indexFieldsTypes.put(field, AggConstants.CAST_BIGINT);
          }
        }
      }
      for (Map.Entry<String, String> next : indexFieldsTypes.entrySet()) {
        indexFields.add(next.getKey());
        indexCastFieldsType.add(SqlUtils.indexCastFieldsType(next.getKey(), next.getValue()));
      }
      allFields.addAll(conditionFields);
      allFields.addAll(indexFields);
      allFields.add(AggConstants.TIMESTAMP);
      String conditionString = conditionFields.toString();
      String indexString = indexFields.toString();
      String indexCastFieldsTypeString = indexCastFieldsType.toString();
      return SqlUtils
          .getSql(aggType, inputPath, conditionString, indexString, indexCastFieldsTypeString);
    } catch (Exception e) {
      LOGGER.error("get common sql error {}", ExceptionUtils.getFullStackTrace(e));
      return "";
    }
  }

  /**
   * 加载 udf 函数.
   *
   * @param session .
   */
  private void initUdf(SparkSession session) {
    session.udf().register(AggConstants.DAY_UDF_NAME, new TimeStampDayTransform(),
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
    session.udf().register(AggConstants.HOUR_UDF_NAME, new TimeStampHourTransform(),
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
  }

  /**
   * 获得 schema .
   *
   * @param fieldNames .
   * @return .
   */
  private StructType getSchema(String[] fieldNames) {
    //new
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : fieldNames) {
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }
    return DataTypes.createStructType(fields);
  }

  /**
   * 读取hdfs service data .
   *
   * @param sc .
   * @param session .
   */
  private void getReadHdfsServiceData(JavaSparkContext sc, SparkSession session) {
    final String commonSql = getCommonSql();
    fieldNames =  allFields.toArray(new String[allFields.size()]);
    notMatchCount = sc.sc().longAccumulator();
    ReadHdfsServiceImpl readHdfsService = new ReadHdfsServiceImpl();
    JavaRDD<Row> rowRdd = readHdfsService.getHdfsDataRdd(sc, inputPath, fieldNames, notMatchCount);
    StructType schema = getSchema(fieldNames);
    Dataset<Row> sourceDs = session.createDataFrame(rowRdd, schema);
    sourceDs.createOrReplaceTempView(AggConstants.SPARK_CACHE_TABLE);
    resultDs = session.sql(commonSql);
    lineRdd = getLineRdd();
  }

  /**
   * 形成 rdd line .
   *
   * @return .
   */
  private JavaRDD<String> getLineRdd() {
    return resultDs.javaRDD().map(row -> {
      Map<String, Object> wordsMap = new HashMap<>();
      for (int index = 0; index < row.size(); index++) {
        if (fieldNames[index].equals(AggConstants.TIMESTAMP)) {
          wordsMap.put(fieldNames[index],
              Long.valueOf(new BigDecimal(row.get(index).toString()).toPlainString()));
        } else {
          wordsMap.put(fieldNames[index], row.get(index));
        }
      }
      wordsMap.put(AggConstants.BUSINESS, businessAgg);
      return GsonUtil.toJson(wordsMap, GsonType.OBJECT_MAP_TYPE);
    });
  }

  /**
   * 发送kafka 数据 .
   *
   * @param sc .
   * @param lineRdd .
   * @throws IOException .
   */
  private void sendKafkaServiceData(JavaSparkContext sc, JavaRDD<String> lineRdd)
      throws IOException {
    matchCount = sc.sc().longAccumulator();
    KafkaServiceImpl kafkaService = new KafkaServiceImpl();
    kafkaService.sendKafka(lineRdd, matchCount);
  }

  /**
   * 写入hdfs 数据，备份 .
   *
   * @param lineRdd .
   * @throws IOException .
   */
  private void sendWriteHdfsServiceData(JavaRDD<String> lineRdd) throws IOException {
    WriteHdfsServiceImpl writeHdfsService = new WriteHdfsServiceImpl();
    writeHdfsService.sendHdfs(lineRdd, outputPath, currentTimeMillis);
  }

  /**
   * 发送 mysql sttaus .
   */
  private void sendMysqlServiceData() {
    MysqlServiceImpl mysqlService = new MysqlServiceImpl();
    mysqlService.sendMysqlAggStatus(businessAgg + "-" + indexYesterday, matchCount.value());
  }

  /**
   * 发送警告 .
   */
  private void sendAlertStatus(boolean flag) {
    try {
      if (flag) {
        WatchAlert.sendAlarmToGroups("sparksql", "summonagg", businessAgg
                + "-" + indexYesterday + " agg success", "count: " + matchCount.value(),
            new String[] {"DIP_SUMMON"});
      } else {
        WatchAlert.sendAlarmToGroups("sparksql", "summonagg", businessAgg
                + "-" + indexYesterday + " agg failed", "agg failed",
            new String[] {"DIP_SUMMON"});
      }
    } catch (Exception e) {
      LOGGER.error("send alert error {}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * 发送警告 .
   */
  private void sendAlertStop() {
    try {
      WatchAlert.sendAlarmToGroups("sparksql", "summonagg", businessAgg
              + "-" + indexYesterday + " agg stop", "agg stop",
          new String[] {"DIP_SUMMON"});
    } catch (Exception e) {
      LOGGER.error("send alert error {}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  @Override
  public void run() {
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);
    try {
      SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();
      initUdf(session);
      getReadHdfsServiceData(sc, session);
      sendKafkaServiceData(sc, lineRdd);
      sendWriteHdfsServiceData(lineRdd);
      sendMysqlServiceData();
      sendAlertStatus(true);
      LOGGER.info("matchCount: {}, not match count: {} indexYesterday: {}", matchCount.value(),
          notMatchCount.value(), indexYesterday);
    } catch (Exception e) {
      LOGGER.error("summon agg service impl error {}", ExceptionUtils.getFullStackTrace(e));
      sendAlertStatus(false);
    } finally {
      sendAlertStop();
      clear();
      sc.stop();
    }

  }

  @Override
  public void clear() {
    indexFields.clear();
    indexFieldsTypes.clear();
    indexCastFieldsType.clear();
    conditionFields.clear();
    allFields.clear();
  }
}
