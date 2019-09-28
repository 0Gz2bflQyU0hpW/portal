package com.weibo.dip.data.platform.datacubic.business.summonagg.udf;

import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by liyang28 on 2018/7/27.
 */
public class TimeStampDayTransform implements UDF1<String, Map<String, String>> {

  private static final String TIMESTAMP_TRANSFORM = AggConstants.TIMESTAMP;
  private static final FastDateFormat SDF1 = FastDateFormat.getInstance("yyyy_MM_dd");

  @Override
  public Map<String, String> call(String along) throws Exception {
    String format = SDF1.format(new Date(Long.valueOf(along)));
    long time = SDF1.parse(format).getTime();
    Map<String, String> result = new HashMap<>();
    result.put(TIMESTAMP_TRANSFORM, String.valueOf(time));
    return result;
  }
}