package com.weibo.dip.data.platform.datacubic.business.util;

/**
 * Created by liyang28 on 2018/7/16.
 */
public class AggConstants {

  public static final String USERNAME = "root";
  public static final String PASSWORD = "mysqladmin";
  private static final String DBNAME = "portal";
  public static final String DRIVER = "com.mysql.jdbc.Driver";
  public static final String URL = "jdbc:mysql://d136092.innet.dip.weibo.com:3307/" + DBNAME
      + "?useServerPrepStmts=false&rewriteBatchedStatements=true";
  private static final String TBNAME = "summon_agg_status";
  public static final String INSERT_SQL =
      "INSERT INTO " + TBNAME + " (`index`, `is_success`, "
          + "`is_alias`, `produce_count`, `consume_count`) VALUES (?,?,?,?,?)";
  public static final String UPDATE_SQL =
      "UPDATE " + TBNAME + " SET `is_success`=?, `is_alias`=?,"
          + " `produce_count`=?, `consume_count`=? WHERE `index`=?";

  public static String getSql(String index) {
    return "SELECT * FROM " + TBNAME + " WHERE `index`=" + "\"" + index + "\"";
  }

  public static final String SPLIT_ARGS = ",";
  public static final String TIMESTAMP = "timestamp";
  public static final String BUSINESS = "business";
  public static final String SUMMON_QUERY_TYPE = "queryType";
  public static final String SUMMON_METADATA = "metadata";
  public static final String SUMMON_DATA_SOURCE = "dataSource";
  public static final String SUMMON_ACCESS_KEY = "accessKey";
  public static final String SUMMON_URL = "http://rest.summon.dip.weibo.com:8079/summon/rest/v2";
  public static final String ACCESS_KEY = "gfpjJQ26D7B9632BEA7F";
  public static final String AGGREGATION = "aggregation";
  public static final String SUMMON_DATA = "data";
  public static final String SUMMON_TYPE = "type";
  public static final String SUMMON_FIELD = "field";
  public static final String SUMMON_STRING = "string";
  public static final String SUMMON_LONG = "long";
  public static final String SUMMON_FLOAT = "float";
  public static final String CAST_BIGINT = "bigint";
  public static final String CAST_DECIMAL = "decimal(28,8)";
  public static final String DAY_UDF_NAME = "timestampDayTransform";
  public static final String HOUR_UDF_NAME = "timestampHourTransform";
  public static final String SPARK_CACHE_TABLE = "summon_agg";
  public static final String DAILY = "daily";
  public static final String HOUR = "hour";
  public static final String DAILY_AGGREAGTION = "daily-aggregation";
  public static final String HOUR_AGGREAGTION = "hour-aggregation";


}
