package com.weibo.dip.platform.hessian;

import com.weibo.dip.web.model.realtime.StreamingInfo;

import java.util.List;

public interface ClientService {

  int start(String appName, String creator);

  int stop(String appName);

  int restart(String appName);

  List<StreamingInfo> list();

  int insert(StreamingInfo streamingInfo);

  int delete(String name);

  int update(StreamingInfo streamingInfo);

  StreamingInfo findByName(String name);

  boolean isExist(String name);

}
