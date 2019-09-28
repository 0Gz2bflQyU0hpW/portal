package com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil.GsonType;
import com.weibo.dip.data.platform.datacubic.business.summonagg.common.HdfsUtils;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.ReadHdfsService;
import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

/**
 * Created by liyang28 on 2018/7/28.
 */
public class ReadHdfsServiceImpl implements ReadHdfsService, Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadHdfsServiceImpl.class);

  public ReadHdfsServiceImpl() {
  }

  @Override
  public  JavaRDD<Row> getHdfsDataRdd(JavaSparkContext sc, String inputPath, String[] fieldNames,
      LongAccumulator notMatchCount) {
    return sc.textFile(inputPath + HdfsUtils.SUFFIX_PATH).map((String line) -> {
      Map<String, Object> json;
      try {
        json = GsonUtil.fromJson(line, GsonType.OBJECT_MAP_TYPE);
        if (MapUtils.isEmpty(json)) {
          LOGGER.error("json null");
          return null;
        }
      } catch (Exception e) {
        notMatchCount.add(1);
        LOGGER
            .error("line {} to json error: {}", line, ExceptionUtils.getFullStackTrace(e));
        return null;
      }
      Object[] values = new String[fieldNames.length];
      for (int index = 0; index < fieldNames.length; index++) {
        if (fieldNames[index].equals(AggConstants.TIMESTAMP)) {
          Object value = json.get(fieldNames[index]);
          values[index] = new BigDecimal(String.valueOf(value)).toPlainString();
        } else {
          Object value = json.get(fieldNames[index]);
          values[index] = (value != null) ? String.valueOf(value) : null;
        }
      }
      return RowFactory.create((Object[]) values);

    }).filter(Objects::nonNull);
  }
}
