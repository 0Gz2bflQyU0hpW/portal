package com.weibo.dip.rest.api;

import com.weibo.dip.data.platform.commons.Symbols;
import com.weibo.dip.rest.bean.Result;
import com.weibo.dip.rest.service.WatchService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Watch controller.
 *
 * @author yurun
 */
@RestController
@RequestMapping("/watch")
public class WatchController {

  private static final Logger LOGGER = LoggerFactory.getLogger(WatchController.class);

  @Autowired private WatchService alarmService;

  /**
   * Send alarm to users.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param users users
   * @return http result
   */
  @RequestMapping(value = "user", method = RequestMethod.POST)
  public Result user(
      @RequestParam String service,
      @RequestParam(name = "subservice") String subService,
      @RequestParam String subject,
      @RequestParam String content,
      @RequestParam String users) {
    try {
      alarmService.sendAlarmToUsers(
          service, subService, subject, content, users.split(Symbols.COMMA));
    } catch (Exception e) {
      LOGGER.error("send alarm to users error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return new Result(HttpStatus.OK);
  }

  /**
   * Send alarm to groups.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param groups groups
   * @return http result
   */
  @RequestMapping(value = "group", method = RequestMethod.POST)
  public Result post(
      @RequestParam String service,
      @RequestParam(name = "subservice") String subService,
      @RequestParam String subject,
      @RequestParam String content,
      @RequestParam String groups) {
    try {
      alarmService.sendAlarmToGroups(
          service, subService, subject, content, groups.split(Symbols.COMMA));
    } catch (Exception e) {
      LOGGER.error("send alarm to groups error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return new Result(HttpStatus.OK);
  }

  /**
   * Alert to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   * @return http result
   */
  @RequestMapping(value = "alert", method = RequestMethod.POST)
  public Result alert(
      @RequestParam String service,
      @RequestParam(name = "subservice") String subService,
      @RequestParam String subject,
      @RequestParam String content,
      @RequestParam String tos) {
    try {
      alarmService.alert(service, subService, subject, content, tos.split(Symbols.COMMA));
    } catch (Exception e) {
      LOGGER.error("alert error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return new Result(HttpStatus.OK);
  }

  /**
   * Report to users or groups, can be used in conjunction.
   *
   * @param service service name
   * @param subService sub service name
   * @param subject subject
   * @param content content
   * @param tos users or groups
   * @return http result
   */
  @RequestMapping(value = "report", method = RequestMethod.POST)
  public Result report(
      @RequestParam String service,
      @RequestParam(name = "subservice") String subService,
      @RequestParam String subject,
      @RequestParam String content,
      @RequestParam String tos) {
    try {
      alarmService.report(service, subService, subject, content, tos.split(Symbols.COMMA));
    } catch (Exception e) {
      LOGGER.error("alert error: {}", ExceptionUtils.getFullStackTrace(e));

      return new Result(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return new Result(HttpStatus.OK);
  }
}
