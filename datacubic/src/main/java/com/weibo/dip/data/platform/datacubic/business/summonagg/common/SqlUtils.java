package com.weibo.dip.data.platform.datacubic.business.summonagg.common;

import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class SqlUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

  /**
   * 获得要转换类型的fields .
   *
   * @param key .
   * @param value .
   */
  public static String indexCastFieldsType(String key, String value) {
    return "cast(sum(" + key + ") as " + value + ") " + key;
  }


  /**
   * get sql .
   *
   * @param aggType .
   * @param inputPath .
   * @param conditionString .
   * @param indexString .
   * @param indexCastFieldsTypeString .
   * @throws IOException .
   */
  public static String getSql(String aggType, String inputPath, String conditionString,
      String indexString, String indexCastFieldsTypeString)
      throws IOException {
    String allTwo = getAllTwoSql(conditionString, indexCastFieldsTypeString);
    if (aggType.equals(AggConstants.DAILY) && HdfsUtils
        .isExistsInputPathAndPathHaveFile(inputPath)) {
      String allOne = getAllOnesql(conditionString, indexString, AggConstants.DAY_UDF_NAME);
      return getAllSql(allOne, allTwo, conditionString);
    } else if (aggType.equals(AggConstants.HOUR) && HdfsUtils
        .isExistsInputPathAndPathHaveFile(inputPath)) {
      String allOne = getAllOnesql(conditionString, indexString, AggConstants.HOUR_UDF_NAME);
      return getAllSql(allOne, allTwo, conditionString);
    } else {
      LOGGER.error("agg type error");
      return "";
    }
  }

  private static String getAllSql(String allOne, String allTwo, String conditionString) {
    return "select " + allTwo + " from (select " + allOne + " from summon_agg) a0"
        + " group by " + conditionString.substring(1, conditionString.length() - 1)
        + ", timestamp";
  }

  private static String getAllOnesql(String conditionString, String indexString,
      String timestampTransform) {
    return conditionString.substring(1, conditionString.length() - 1) + ", "
        + indexString.substring(1, indexString.length() - 1)
        + ", " + timestampTransform + "(timestamp)[\"timestamp\"] timestamp";
  }

  private static String getAllTwoSql(String conditionString, String indexCastFieldsTypeString) {
    return conditionString.substring(1, conditionString.length() - 1) + ", "
        + indexCastFieldsTypeString.substring(1, indexCastFieldsTypeString.length() - 1)
        + ", timestamp";
  }


}
