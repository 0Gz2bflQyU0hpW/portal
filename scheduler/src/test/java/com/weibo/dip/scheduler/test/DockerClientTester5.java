package com.weibo.dip.scheduler.test;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientTester5 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DockerClientTester5.class);

  public static void main(String[] args) throws Exception {
    DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    List<String> logs = new ArrayList<>();

    dockerClient
        .logContainerCmd("b377a02651ec")
        .withStdOut(true)
        .withStdErr(true)
        .withTailAll()
        .exec(
            new LogContainerResultCallback() {
              @Override
              public void onNext(Frame item) {
                logs.add(item.toString());
              }
            })
        .awaitCompletion();

    for (String log : logs) {
      LOGGER.info(log);
    }
  }
}
