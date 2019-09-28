package com.weibo.dip.baishanyundownload.log;

import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class DomainLogDetailServiceTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainLogDetailServiceTester.class);

  public static void main(String[] args) {
    DomainLogDetailService service =
        new DomainLogDetailService("8174d424368bdd792d7264a76e624129", 24);

    List<DomainLogDetail> logs =
        service.getLogs(
            Collections.singletonList("ww1.sinaimg.cn"), "2018-08-03 00:00", "2018-08-03 09:00");

    if (CollectionUtils.isNotEmpty(logs)) {
      for (DomainLogDetail log : logs) {
        LOGGER.info(log.toString());
      }
    }
  }
}
