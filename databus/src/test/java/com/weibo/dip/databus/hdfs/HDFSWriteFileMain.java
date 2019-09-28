package com.weibo.dip.databus.hdfs;

import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by yurun on 17/12/20.
 */
public class HDFSWriteFileMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSWriteFileMain.class);

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                fs.create(new Path("/tmp/yurun/test_file"), true),
                CharEncoding.UTF_8));

        int count = 0;

        while (true) {
            String data = String.valueOf(System.currentTimeMillis());

            writer.write(data);
            writer.newLine();

            writer.flush();

            LOGGER.info("write line: " + data);

            if(++count > 10) {
                break;
            }

            try {
                Thread.sleep(3 * 1000);
            } catch (InterruptedException e) {
            }
        }

        writer.close();
    }

}
