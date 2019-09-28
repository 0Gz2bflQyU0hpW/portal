package com.weibo.dip.iplibrary;

import com.weibo.dip.data.platform.commons.util.IPUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ip library performance tester.
 *
 * @author yurun
 */
public class FixedIpDiskPerformanceTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedIpDiskPerformanceTester.class);

  private static boolean stoped = false;

  private static final AtomicLong COUNT = new AtomicLong(0);

  private static final IpLibrary IP_LIBRARY;

  static {
    try {
      IP_LIBRARY = new IpLibrary();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final List<String> IPS = new ArrayList<>();

  private static class Threader extends Thread {

    @Override
    public void run() {
      Random random = ThreadLocalRandom.current();

      while (!stoped) {
        IP_LIBRARY.getLocation(IPS.get(random.nextInt(IPS.size())));

        COUNT.incrementAndGet();
      }
    }
  }

  public static void main(String[] args) {
    int ips = Integer.valueOf(args[0]);

    int count = 0;
    while (++count <= ips) {
      IPS.add(IPUtil.randomIp());
    }

    int threads = Integer.valueOf(args[1]);

    for (int index = 0; index < threads; index++) {
      new Threader().start();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  stoped = true;

                  try {
                    Thread.sleep(5 * 1000);

                    IP_LIBRARY.close();
                  } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                  }
                }));

    while (!stoped) {
      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        LOGGER.warn(e.getMessage());
      }

      System.out.println(COUNT.getAndSet(0) / 5);
    }
  }
}
