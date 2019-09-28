package com.weibo.dip.scheduler.test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.core.DockerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientTester2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerClientTester2.class);

  public static void main(String[] args) throws Exception {
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    CreateContainerResponse container =
        dockerClient
            .createContainerCmd("dip:0.0.1")
            .withCmd("sh", "start.sh")
            .withName("test_centos")
            .withBinds(Bind.parse("/tmp:/tmp"))
            .exec();

    try {
      LOGGER.info("begin to exec");
      dockerClient.startContainerCmd(container.getId()).exec();
      LOGGER.info("end to exec");

      Thread.sleep(3);
    } finally {
      dockerClient.removeContainerCmd(container.getId()).exec();
    }
  }
}
