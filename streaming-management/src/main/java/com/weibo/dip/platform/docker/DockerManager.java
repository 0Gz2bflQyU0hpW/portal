package com.weibo.dip.platform.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.core.command.PullImageResultCallback;
import com.weibo.dip.data.platform.commons.Symbols;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerManager.class);

  private static final String EXITED = "exited";
  private static final String WORK_SPACE_PART = "/data0/workspace/";
  private static final String SUBMIT_PART = WORK_SPACE_PART + "common/submit.sh";
  private static final int PULL_IMAGE_RETRY_TIMES = 2;
  private static final int TIMEOUT = 30;

  private static DockerClient dockerClient = DockerClientBuilder.getInstance().build();

  /**
   * pull image with name and tag.
   *
   * @param imageName image name
   * @param tag tag
   * @return success or not
   */
  public boolean pullImage(String imageName, String tag) {
    boolean isExist = existImage(imageName, tag);
    if (isExist) {
      LOGGER.info("image {} already exist.", imageName + Symbols.COLON + tag);
      return true;
    }
    boolean flag = false;
    for (int times = 0; times < PULL_IMAGE_RETRY_TIMES; times++) { // 读配置文件，有默认值
      LOGGER.info("try to pull image {} {} time", imageName + Symbols.COLON + tag, times + 1);
      try {
        flag =
            dockerClient
                .pullImageCmd(imageName)
                .withTag(tag)
                .exec(new PullImageResultCallback())
                .awaitCompletion(TIMEOUT, TimeUnit.SECONDS);
        if (flag) {
          LOGGER.info("pull image {} {} time, success.", imageName + Symbols.COLON + tag, times);
          break;
        }
      } catch (DockerException e) { // dockerclient docker网络异常在那一层处理，docker daemon
        LOGGER.error(
            "pull image {} error. {}",
            (imageName + Symbols.COLON + tag),
            ExceptionUtils.getFullStackTrace(e));
        break;
      } catch (InterruptedException e) {
        LOGGER.error(
            "pull image {} error. {}",
            (imageName + Symbols.COLON + tag),
            ExceptionUtils.getFullStackTrace(e));
      }
      if (times == PULL_IMAGE_RETRY_TIMES - 1) {
        LOGGER.error(
            "pull image {} {} times, failed.",
            (imageName + Symbols.COLON + tag),
            PULL_IMAGE_RETRY_TIMES);
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        LOGGER.error(
            "pull image {} error. {}",
            (imageName + Symbols.COLON + tag),
            ExceptionUtils.getFullStackTrace(e));
      }
    }
    return flag;
  }

  /**
   * create contrainer.
   *
   * @param imageName image name
   * @param tag image tag
   * @param containerName app name
   * @return the container's info
   */
  public CreateContainerResponse createContainer(
      String imageName, String tag, String containerName) {
    CreateContainerResponse container = null;
    try { // 异常抛出到应用层
      container =
          dockerClient
              .createContainerCmd(imageName + Symbols.COLON + tag)
              .withEntrypoint(SUBMIT_PART, containerName)
              .withName(containerName)
              .withBinds(
                  Bind.parse(
                      WORK_SPACE_PART
                          + containerName
                          + Symbols.COLON
                          + WORK_SPACE_PART
                          + containerName))
              .exec();
    } catch (DockerException e) {
      LOGGER.error(
          "create container {} error. {}", containerName, ExceptionUtils.getFullStackTrace(e));
    }
    return container;
  }

  /**
   * startcontainer and exec submit.sh after get the exit code.
   *
   * @param container container
   * @param containerName container name
   * @return exitcode
   */
  public int startContainer(CreateContainerResponse container, String containerName) {
    int exitCode = -1;
    try {
      dockerClient.startContainerCmd(container.getId()).exec();
      LOGGER.info("starting Container {}", containerName);
      boolean flag = true;
      while (flag) {
        String status =
            dockerClient.inspectContainerCmd(container.getId()).exec().getState().getStatus();
        if (EXITED.equals(status)) {
          LOGGER.info("container {} exited", containerName);
          flag = false;
        } else {
          TimeUnit.SECONDS.sleep(1);
        }
      }
      exitCode =
          dockerClient.inspectContainerCmd(container.getId()).exec().getState().getExitCode();
    } catch (Exception e) {
      LOGGER.error(ExceptionUtils.getFullStackTrace(e));
    } finally {
      try {
        dockerClient.removeContainerCmd(container.getId()).exec();
        LOGGER.info("remove container {}, contianerId: {}", containerName, container.getId());
      } catch (NotFoundException e) {
        LOGGER.error(
            "remove container {} error. {}", containerName, ExceptionUtils.getFullStackTrace(e));
      }
    }
    return exitCode;
  }

  private boolean existImage(String imageName, String tag) {
    List<Image> images = dockerClient.listImagesCmd().exec();
    boolean isExist = false;
    if (images == null || images.size() == 0) {
      return isExist;
    }
    for (Image image : images) {
      String[] repoTags = image.getRepoTags();
      if (repoTags.length > 0 && repoTags[0].equals(imageName + Symbols.COLON + tag)) {
        isExist = true;
      }
    }
    return isExist;
  }

  /**
   * remove container by name.
   *
   * @param containerName container name
   */
  public void removeContainer(String containerName) {
    List<Container> containers = dockerClient.listContainersCmd().exec();
    if (CollectionUtils.isEmpty(containers)) {
      return;
    }
    boolean flag = false;
    for (Container container : containers) {
      // 获取名称的方法
      String names = container.getNames()[0];
      String name = names.substring(1, names.length());
      if (name.equals(containerName)) {
        if (!EXITED.equals(container.getStatus())) {
          try {
            dockerClient.stopContainerCmd(container.getId()).exec();
            LOGGER.info("stop container {}", containerName);
          } catch (Exception e) {
            LOGGER.error(
                "stop container {} error. {}", containerName, ExceptionUtils.getFullStackTrace(e));
          }
        }
        try {
          dockerClient.removeContainerCmd(container.getId()).exec(); // 抛出异常
          flag = true;
          LOGGER.info("remove container {}", containerName);
        } catch (NotFoundException nfe) {
          LOGGER.error(
              "remove container {} error. {}",
              containerName,
              ExceptionUtils.getFullStackTrace(nfe));
        }
      }
    }
    if (!flag) {
      LOGGER.info("remove container {} failed", containerName);
    }
  }

  /**
   * get log by container id.
   *
   * @param containerId conteiner id
   * @return log
   */
  public String getLog(String containerId) {
    List<String> logs = new ArrayList<>();
    try {
      dockerClient
          .logContainerCmd(containerId)
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
    } catch (InterruptedException e) {
      LOGGER.error(
          "get container error. containerId {} {}",
          containerId,
          ExceptionUtils.getFullStackTrace(e));
    }
    return StringUtils.join(logs, Symbols.NEWLINE);
  }
}
