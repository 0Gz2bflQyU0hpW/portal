package com.weibo.dip.scheduler.test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.PullImageResultCallback;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerClientTester.class);

  public static void main(String[] args) throws Exception {
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    List<Image> images =
        dockerClient
            .listImagesCmd()
            .withImageNameFilter("registry.api.weibo.com/dippub/dip_centos_0.0.1")
            .exec();

    LOGGER.info("images: {}", images.size());

    for (Image image : images) {
      LOGGER.info("image: {}", image.toString());
    }

    boolean flag =
        dockerClient
            .pullImageCmd("registry.api.weibo.com/dippub/dip_centos")
            .withTag("0.0.1")
            .exec(new PullImageResultCallback())
            .awaitCompletion(3600, TimeUnit.SECONDS);

    LOGGER.info("result: {}", flag);
  }
}
