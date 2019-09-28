package com.weibo.dip;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/11/30.
 */
public class ShutdownMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownMain.class);

    private static class Timer extends Thread {

        {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info(Thread.currentThread().getName() + " executed");
            }));
        }

        private boolean stoped = false;

        public void setStoped(boolean stoped) {
            this.stoped = stoped;
        }

        @Override
        public void run() {
            while (!stoped) {
                LOGGER.info("time: {}", System.currentTimeMillis());

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    LOGGER.error("while interrupted");
                }
            }

            LOGGER.info("Flag is stoped");

            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
            }

            LOGGER.info("server stoped");
        }

    }

    public static void main(String[] args) {
        Timer timer = new Timer();

        timer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            LOGGER.info(Thread.currentThread().getName() + " executed")));

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            LOGGER.info(Thread.currentThread().getName() + " executed")));
    }

}
