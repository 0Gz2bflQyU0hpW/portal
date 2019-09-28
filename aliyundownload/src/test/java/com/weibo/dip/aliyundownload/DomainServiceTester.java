package com.weibo.dip.aliyundownload;

import com.weibo.dip.aliyundownload.domain.Domain;
import com.weibo.dip.aliyundownload.domain.DomainService;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class DomainServiceTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainServiceTester.class);

  public static void main(String[] args) throws Exception {
    String accessKeyId = "LTAIxKzl1eARmD9r";
    String accessKeySecret = "tiPsFAfnqN8cBg2SLiCclQzGf5MpzC";

    DomainService domainService = new DomainService(accessKeyId, accessKeySecret);

    List<Domain> domains = domainService.getDomains();

    List<String> onlineDomains = new ArrayList<>();

    for (Domain domain : domains) {
      //      if (domain.getDomainStatus().equals("online")) {
      //        LOGGER.info(domain.getDomainName());
      //
      //        onlineDomains.add(domain.getDomainName());
      //      }
      onlineDomains.add(domain.getDomainName());
    }

    LOGGER.info("domains: {}", onlineDomains.size());
  }
}
