package com.weibo.dip.streaming;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator tester.
 *
 * @author yurun
 */
public class CuratorTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(CuratorTester.class);

  public static void main(String[] args) {
    int baseSleepTimeMs = 1000;
    int maxRetries = 1000;

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);

    String connectionString = "10.13.4.44:2180";

    CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);

    client.start();

    String leaderElectionPath = "/curator/leader";

    LeaderSelector leaderSelector =
        new LeaderSelector(
            client,
            leaderElectionPath,
            new LeaderSelectorListenerAdapter() {
              @Override
              public void takeLeadership(CuratorFramework client) {
                LOGGER.info("I'm leader!");

                try {
                  Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
              }
            });

    leaderSelector.autoRequeue();

    leaderSelector.start();

    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }
}
