package com.weibo.dip.rest.service;

/**
 * Watch service.
 *
 * @author yurun
 */
public interface WatchService {
  /**
   * 发送报警信息至用户，报警信息的各部分不可以包含引号（"）.
   *
   * @param service 服务名称
   * @param subService 子服务名称
   * @param subject 标题
   * @param content 内容（注：私信不显示内容）
   * @param users 用户列表
   */
  void sendAlarmToUsers(
      String service, String subService, String subject, String content, String[] users)
      throws Exception;

  /**
   * 发送报警信息至用户组，报警信息的各部分不可以包含引号（"）.
   *
   * @param service 服务名称
   * @param subService 子服务名称
   * @param subject 标题
   * @param content 内容（注：私信不显示内容）
   * @param groups 用户组列表
   */
  void sendAlarmToGroups(
      String service, String subService, String subject, String content, String[] groups)
      throws Exception;

  /**
   * 发送报警至用户/用户组（可联合使用），包括：短信（私信）、邮件、微信.
   *
   * @param service 服务名称
   * @param subService 子服务名称
   * @param subject 标题
   * @param content 内容（注：私信不显示内容）
   * @param tos 用户/用户组列表
   */
  void alert(String service, String subService, String subject, String content, String[] tos)
      throws Exception;

  /**
   * 发送报表信息至用户/用户组（可联合使用），仅包括：邮件.
   *
   * @param service 服务名称
   * @param subService 子服务名称
   * @param subject 标题
   * @param content 内容（注：私信不显示内容）
   * @param tos 用户/用户组列表
   */
  void report(String service, String subService, String subject, String content, String[] tos)
      throws Exception;
}
