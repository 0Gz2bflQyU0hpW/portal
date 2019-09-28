package com.weibo.dip.web.service;

import com.weibo.dip.web.model.realtime.StreamingInfo;

import java.util.List;

public interface StreamingAppService {
  /**
   * start.
   *
   * @param appName app name
   */
  int start(String appName,String creator);

  /**
   * stop.
   *
   * @param appName app name
   */
  int stop(String appName);

  /**
   * restart.
   *
   * @param appName app name
   */
  int restart(String appName);

  /**
   * 搜索所有的app.
   *
   * @return list
   */
  List<StreamingInfo> list();

  /**
   * 添加应用信息.
   *
   * @param streamingInfo info
   * @return i
   */
  int insert(StreamingInfo streamingInfo);

  /**
   * 删除应用信息.
   *
   * @param name name
   * @return i
   */
  int delete(String name);

  /**
   * 更新应用信息.
   *
   * @param streamingInfo info
   * @return i
   */
  int update(StreamingInfo streamingInfo);

  /**
   * find by name.
   *
   * @param name name
   */
  StreamingInfo findByName(String name);

  /** 根据appName判定此名称是否被占用 1：yes 0：no. */
  boolean isExit(String appName);
}
