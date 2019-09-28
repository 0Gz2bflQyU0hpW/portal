package com.weibo.dip.web.service.impl;

import com.weibo.dip.web.model.realtime.RpcService;
import com.weibo.dip.web.model.realtime.StreamingInfo;
import com.weibo.dip.web.service.StreamingAppService;
import java.util.List;

import org.springframework.stereotype.Service;

@Service
public class StreamingAppServiceImpl implements StreamingAppService {

  @Override
  public int start(String appName,String creator) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.start(appName,creator);
  }

  @Override
  public int stop(String appName) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.stop(appName);
  }

  @Override
  public int restart(String appName) {

    RpcService rpcService = RpcService.getRpcService();
    return rpcService.restart(appName);
  }

  @Override
  public List<StreamingInfo> list() {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.list();
  }

  @Override
  public int insert(StreamingInfo streamingInfo) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.insert(streamingInfo);
  }

  @Override
  public int delete(String name) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.delete(name);
  }

  @Override
  public int update(StreamingInfo streamingInfo) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.update(streamingInfo);
  }

  @Override
  public StreamingInfo findByName(String name) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.findByName(name);
  }

  @Override
  public boolean isExit(String appName) {
    RpcService rpcService = RpcService.getRpcService();
    return rpcService.isExist(appName);
  }
}
