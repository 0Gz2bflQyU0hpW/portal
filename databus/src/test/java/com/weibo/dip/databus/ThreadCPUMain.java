package com.weibo.dip.databus;

/**
 * Created by yurun on 17/9/21.
 */
public class ThreadCPUMain {

    private static class CPU extends Thread {

        @Override
        public void run() {
            while (true) {

            }
        }

    }

    public static void main(String[] args) {
        int threads = Integer.valueOf(args[0]);

        int index = 0;

        while (index < threads) {
            new CPU().start();

            index++;
        }

    }

}
