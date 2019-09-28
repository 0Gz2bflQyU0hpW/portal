package com.weibo.dip.data.platform.datacubic.launcher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 17/3/24.
 */
public class LauncherMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(LauncherMain.class);

    public static void main(String[] args) throws Exception {
        SparkLauncher launcher = new SparkLauncher();

        launcher.setJavaHome("/usr/local/jdk1.8.0_45");
        launcher.setSparkHome("/usr/local/spark-2.0.1-bin-2.5.0-cdh5.3.2");

        launcher.setMaster("local");
        launcher.setAppResource("/data0/workspace/portal/datacubic/target/datacubic-2.0.0-SNAPSHOT-jar-with-dependencies.jar");
        launcher.setMainClass(WordCountMain.class.getName());

        SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {

            @Override
            public void stateChanged(SparkAppHandle handle) {

            }

            @Override
            public void infoChanged(SparkAppHandle handle) {

            }

        });

        while (true) {
            //System.out.println(handle.getState().name());

            Thread.sleep(3000);
        }
    }

}
