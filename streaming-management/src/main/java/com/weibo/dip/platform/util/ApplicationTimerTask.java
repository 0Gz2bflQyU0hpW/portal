package com.weibo.dip.platform.util;

import com.weibo.dip.platform.model.StateEnum;
import com.weibo.dip.platform.model.Streaming;
import com.weibo.dip.platform.mysql.StreamingdbUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationTimerTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationTimerTask.class);

  private static List<Streaming> streamings = new ArrayList<>();
  private static StreamingdbUtil streamingdbUtil = new StreamingdbUtil();

  static {
    Timer timer = new Timer("ApplicationTimerTask");
    timer.schedule(new Task(), 3000L, 1000L);
  }

  private static class Task extends TimerTask {

    @Override
    public void run() {
      if (streamings.size() == 0) {
        streamings.addAll(streamingdbUtil.listWait());
        for (Streaming streaming : streamings) {
          if (StateEnum.WAITSTART.getNumber() == streaming.getState()) {
            int result =
                streamingdbUtil
                    .updateState(streaming.getId(), StateEnum.STARTPOOL.getNumber(),
                        Arrays.asList(StateEnum.WAITSTART.getNumber()));
            if (result == 1) {
              LOGGER.info("begin starting application {}", streaming.getAppName());
              ThreadUtil.startApplication(streaming);
            }
          } else if (StateEnum.WAITKILL.getNumber() == streaming.getState()) {
            int result =
                streamingdbUtil
                    .updateState(streaming.getId(), StateEnum.KILLING.getNumber(),
                        Arrays.asList(StateEnum.WAITKILL.getNumber()));
            if (result == 1) {
              LOGGER.info("begin killing application {}", streaming.getAppName());
              ThreadUtil.killApplication(streaming);
            }
          } else if (StateEnum.WAITRESTART.getNumber() == streaming.getState()) {
            int result = streamingdbUtil
                .updateState(streaming.getId(), StateEnum.RESTARTING.getNumber(),
                    Arrays.asList(StateEnum.WAITRESTART.getNumber()));
            if (result == 1) {
              LOGGER.info("begin restarting application {}", streaming.getAppName());
              ThreadUtil.restartApplication(streaming);
            }
          }
        }
        streamings.clear();
      }
    }
  }

}
