package com.weibo.dip.scheduler.test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientTester6 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerClientTester6.class);

  public static void main(String[] args) throws Exception {
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    List<String> logs = new ArrayList<>();

    List<Container> containers =
        dockerClient.listContainersCmd().withNameFilter(Collections.singleton(args[0])).exec();

    for (Container container : containers) {
      System.out.println(container.getId());
    }
  }
}
