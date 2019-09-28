package com.weibo.dip.data.platform.datacubic.watch;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/1/24.
 */
public class HDFSSelectJobResultClearn {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSSelectJobResultClearn.class);

    private static final Pattern PATTERN = Pattern.compile(".*_(\\d{8})\\d*");

    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {
        Calendar calendar = Calendar.getInstance();

        calendar.setTimeInMillis(System.currentTimeMillis());

        calendar.add(Calendar.DAY_OF_YEAR, -7);

        Date endline = calendar.getTime();

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        String result = "/user/hdfs/result/selectjob";

        FileStatus[] statuses = fs.listStatus(new Path(result));
        if (ArrayUtils.isEmpty(statuses)) {
            LOGGER.warn("statuses is empty");

            return;
        }

        Path[] jobIdDirs = FileUtil.stat2Paths(statuses);

        for (Path jobIdDir : jobIdDirs) {
            statuses = fs.listStatus(jobIdDir);
            if (ArrayUtils.isEmpty(statuses)) {
                fs.delete(jobIdDir, true);

                LOGGER.warn("Empty: " + jobIdDir + ", remove");

                continue;
            }

            Path[] jobRecordDirs = FileUtil.stat2Paths(statuses);

            for (Path jobRecordDir : jobRecordDirs) {
                String jobRecordName = jobRecordDir.getName();

                Matcher matcher = PATTERN.matcher(jobRecordName);

                if (matcher.matches()) {
                    Date jobRecordDate = DAY_FORMAT.parse(matcher.group(1));

                    if (jobRecordDate.compareTo(endline) < 0) {
                        fs.delete(jobRecordDir,true);

                        LOGGER.info("Expired: " + jobRecordDir + ", remove");
                    }
                } else {
                    LOGGER.warn("Error: " + jobRecordDir);
                }
            }
        }
    }

}
