package com.weib.dip.data.platform.services.client;

/**
 * Created by yurun on 16/12/21.
 */
public interface AlarmService {

    /**
     * 发送报警信息至用户，报警信息的各部分不可以包含引号（"）
     *
     * @param service    服务名称
     * @param subService 子服务名称
     * @param subject    标题
     * @param content    内容（注：私信不显示内容）
     * @param users      用户列表
     */
    void sendAlarmToUsers(String service, String subService, String subject, String content, String[] users);

    /**
     * 发送报警信息至用户组，报警信息的各部分不可以包含引号（"）
     *
     * @param service    服务名称
     * @param subService 子服务名称
     * @param subject    标题
     * @param content    内容（注：私信不显示内容）
     * @param groups     用户组列表
     */
    void sendAlarmToGroups(String service, String subService, String subject, String content, String[] groups);

}
