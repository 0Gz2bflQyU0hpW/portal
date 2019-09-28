package com.weibo.dip.data.platform.datacubic.business.summonagg;

import com.weibo.dip.data.platform.datacubic.business.summonagg.common.CheckoutParams;
import com.weibo.dip.data.platform.datacubic.business.summonagg.service.impl.SparkSqlServiceImpl;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liyang28 on 2018/7/27.
 */
public class SummonAggApplication {

  private static final Logger LOGGER = LoggerFactory.getLogger(SummonAggApplication.class);

  /**
   * Summon agg start .
   *
   * @param args .
   */
  public void start(String[] args) {
    try {
      CheckoutParams.checkoutArgs(args);
      new SparkSqlServiceImpl(args).run();
    } catch (Exception e) {
      LOGGER.error("summon agg application error {}", ExceptionUtils.getFullStackTrace(e));
    }

  }

}
