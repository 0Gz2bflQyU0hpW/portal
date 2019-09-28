package com.weibo.dip.scheduler.test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientTester3 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerClientTester3.class);

  public static void main(String[] args) {
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    List<Container> containers = dockerClient.listContainersCmd().withShowAll(true).exec();

    for (Container container : containers) {
      LOGGER.info("lables: {}", container.getLabels());
    }
  }
}
