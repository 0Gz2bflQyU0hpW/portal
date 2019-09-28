package com.weibo.dip.data.platform.commons.util;

import com.weibo.dip.data.platform.commons.Symbols;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Created by yurun on 17/6/19. */
public class WatchAlert {
  private static final Logger LOGGER = LoggerFactory.getLogger(WatchAlert.class);

  private static final String WATCH_USER_REST = "http://api.dip.weibo.com:9083/watch/user";
  private static final String WATCH_GROUP_REST = "http://api.dip.weibo.com:9083/watch/group";

  private static final String WATCH_ALERT_REST = "http://api.dip.weibo.com:9083/watch/alert";
  private static final String WATCH_REPORT_REST = "http://api.dip.weibo.com:9083/watch/report";

  private static final String SERVICE = "service";
  private static final String SUBSERVICE = "subservice";
  private static final String SUBJECT = "subject";
  private static final String CONTENT = "content";

  private static final String USERS = "users";
  private static final String GROUPS = "groups";

  private static final String TOS = "tos";

  private static final String CODE = "code";
  private static final int CODE_OK = 200;

  /**
   * Send alarm to users.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param users users
   * @throws Exception if send alarm error
   */
  public static void sendAlarmToUsers(
      String service, String subService, String subject, String content, String[] users)
      throws Exception {
    Map<String, String> params = new HashMap<>();

    params.put(SERVICE, service);
    params.put(SUBSERVICE, subService);
    params.put(SUBJECT, subject);
    params.put(CONTENT, content);
    params.put(USERS, StringUtils.join(users, Symbols.COMMA));

    String response = HttpClientUtil.doPost(WATCH_USER_REST, params);

    LOGGER.debug("response: {}", response);

    Map<String, Object> results = GsonUtil.fromJson(response, GsonUtil.GsonType.OBJECT_MAP_TYPE);

    if (!results.containsKey(CODE) || ((Double) results.get(CODE)).intValue() != CODE_OK) {
      throw new RuntimeException("send alarm to users error: " + response);
    }
  }

  /**
   * Send alarm to groups.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param groups gruops
   * @throws Exception if send alarm error
   */
  public static void sendAlarmToGroups(
      String service, String subService, String subject, String content, String[] groups)
      throws Exception {
    Map<String, String> params = new HashMap<>();

    params.put(SERVICE, service);
    params.put(SUBSERVICE, subService);
    params.put(SUBJECT, subject);
    params.put(CONTENT, content);
    params.put(GROUPS, StringUtils.join(groups, Symbols.COMMA));

    String response = HttpClientUtil.doPost(WATCH_GROUP_REST, params);

    LOGGER.debug("response: {}", response);

    Map<String, Object> results = GsonUtil.fromJson(response, GsonUtil.GsonType.OBJECT_MAP_TYPE);

    if (!results.containsKey(CODE) || ((Double) results.get(CODE)).intValue() != CODE_OK) {
      throw new RuntimeException("send alarm to users error: " + response);
    }
  }

  /**
   * Alert to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   * @throws Exception if alert error
   */
  public static void alert(
      String service, String subService, String subject, String content, String[] tos)
      throws Exception {
    Map<String, String> params = new HashMap<>();

    params.put(SERVICE, service);
    params.put(SUBSERVICE, subService);
    params.put(SUBJECT, subject);
    params.put(CONTENT, content);
    params.put(TOS, StringUtils.join(tos, Symbols.COMMA));

    String response = HttpClientUtil.doPost(WATCH_ALERT_REST, params);

    LOGGER.debug("response: {}", response);

    Map<String, Object> results = GsonUtil.fromJson(response, GsonUtil.GsonType.OBJECT_MAP_TYPE);

    if (!results.containsKey(CODE) || ((Double) results.get(CODE)).intValue() != CODE_OK) {
      throw new RuntimeException("send alarm to users error: " + response);
    }
  }

  /**
   * Report to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   * @throws Exception if report error
   */
  public static void report(
      String service, String subService, String subject, String content, String[] tos)
      throws Exception {
    Map<String, String> params = new HashMap<>();

    params.put(SERVICE, service);
    params.put(SUBSERVICE, subService);
    params.put(SUBJECT, subject);
    params.put(CONTENT, content);
    params.put(TOS, StringUtils.join(tos, Symbols.COMMA));

    String response = HttpClientUtil.doPost(WATCH_REPORT_REST, params);

    LOGGER.debug("response: {}", response);

    Map<String, Object> results = GsonUtil.fromJson(response, GsonUtil.GsonType.OBJECT_MAP_TYPE);

    if (!results.containsKey(CODE) || ((Double) results.get(CODE)).intValue() != CODE_OK) {
      throw new RuntimeException("send alarm to users error: " + response);
    }
  }
}
