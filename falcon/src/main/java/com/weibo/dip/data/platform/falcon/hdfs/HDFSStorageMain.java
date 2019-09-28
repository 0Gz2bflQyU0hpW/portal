package com.weibo.dip.data.platform.falcon.hdfs;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Created by yurun on 17/4/17.
 */
public class HDFSStorageMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] statuses = fs.listStatus(new Path(args[0]));

        if (ArrayUtils.isEmpty(statuses)) {
            return;
        }

        Path[] paths = FileUtil.stat2Paths(statuses);

        long sum = 0;

        for (Path path : paths) {
            long length = fs.getContentSummary(path).getLength();

            sum += length;

            System.out.println((path + "\t" + length));
        }

        System.out.print("sum: " + sum);
    }

}
