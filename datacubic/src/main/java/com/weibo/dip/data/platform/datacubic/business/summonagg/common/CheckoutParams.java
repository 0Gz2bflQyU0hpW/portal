package com.weibo.dip.data.platform.datacubic.business.summonagg.common;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil.GsonType;
import com.weibo.dip.data.platform.datacubic.business.util.AggConstants;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/27.
 */
public class CheckoutParams {

  private static final Logger LOGGER = LoggerFactory.getLogger(CheckoutParams.class);
  private static final Long currentTimeMillis = System.currentTimeMillis();

  /**
   * 校验 args 参数 .
   *
   * @param args .
   * @throws Exception .
   */
  public static void checkoutArgs(String[] args) throws Exception {
    if (args.length == 0) {
      throw new Exception("args is empty");
    } else {
      String[] split = args[0].split(AggConstants.SPLIT_ARGS);
      if (split.length == 2) {
        if (checkoutSummonApiResult(split[0])) {
          throw new Exception("args split[0] content is error" + split[0]);
        }
        if (!(split[1].equals(AggConstants.DAILY) || split[1].equals(AggConstants.HOUR))) {
          throw new Exception("args split[1] content is error" + split[1]);
        }
      } else {
        throw new Exception("args length split length is not 2, length is " + args.length);
      }
    }
  }

  /**
   * 校验 summon business .
   *
   * @param business .
   * @return .
   */
  private static boolean checkoutSummonApiResult(String business) {
    try {
      String summonRespResult = SummonApi.getSummonRespResult(business, currentTimeMillis);
      Map<String, Object> mapResult = GsonUtil
          .fromJson(summonRespResult,
              GsonType.OBJECT_MAP_TYPE);
      Object data = mapResult.getOrDefault(AggConstants.SUMMON_DATA, AggConstants.SUMMON_DATA);
      return Objects.isNull(data) || data.equals(AggConstants.SUMMON_DATA);
    } catch (Exception e) {
      LOGGER.error("checkout summon api error {}", ExceptionUtils.getFullStackTrace(e));
      return true;
    }
  }
}
