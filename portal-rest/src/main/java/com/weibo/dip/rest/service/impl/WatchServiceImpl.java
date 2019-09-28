package com.weibo.dip.rest.service.impl;

import com.weibo.dip.data.platform.commons.SinaWatchClient;
import com.weibo.dip.rest.service.WatchService;
import org.springframework.stereotype.Service;

/**
 * Watch service impl.
 *
 * @author yurun
 */
@Service
public class WatchServiceImpl implements WatchService {
  private SinaWatchClient watchClient = new SinaWatchClient();

  @Override
  public void sendAlarmToUsers(
      String service, String subService, String subject, String content, String[] users) {
    watchClient.alert(service, subService, subject, content, users);
  }

  @Override
  public void sendAlarmToGroups(
      String service, String subService, String subject, String content, String[] groups) {
    watchClient.alert(service, subService, subject, content, groups);
  }

  @Override
  public void alert(
      String service, String subService, String subject, String content, String[] tos) {
    watchClient.alert(service, subService, subject, content, tos);
  }

  @Override
  public void report(
      String service, String subService, String subject, String content, String[] tos) {
    watchClient.report(service, subService, subject, content, tos);
  }
}
