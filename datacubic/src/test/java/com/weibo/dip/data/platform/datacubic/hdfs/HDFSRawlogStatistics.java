package com.weibo.dip.data.platform.datacubic.hdfs;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by yurun on 17/5/17.
 */
public class HDFSRawlogStatistics {

    private static final SimpleDateFormat YYYYMMDDHHMMSS = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] args) throws Exception {
        List<Path> datasets = HDFSUtil.listDirs("/user/hdfs/rawlog", false);

        File dir = new File("/data0/yurun/temp/rawlog");

        BufferedWriter writer = null;

        int count = 0;

        while (true) {
            String timestamp = YYYYMMDDHHMMSS.format(new Date());

            if (writer == null) {
                writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(dir, timestamp)), CharEncoding.UTF_8));
            }

            for (Path path : datasets) {
                String dataset = path.getName();

                long length = HDFSUtil.summary(path).getLength();

                writer.write(dataset + "\t" + timestamp + "\t" + length);
                writer.newLine();
            }

            writer.flush();

            Thread.sleep(60 * 1000);
            if (++count > 60) {
                count = 0;

                writer.close();
                writer = null;
            }
        }

    }

}
