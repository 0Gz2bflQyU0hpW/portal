package com.weibo.dip.web.model.realtime;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/** Created by haisen on 2018/8/22. */
@Component
public class InitRpcService implements CommandLineRunner {
  @Override
  public void run(String... strings) throws Exception {
    RpcService.getRpcService();
  }
}
