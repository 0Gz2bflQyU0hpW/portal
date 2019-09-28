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
public class HDFSJobResultClearn {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSJobResultClearn.class);

    private static final Pattern PATTERN = Pattern.compile(".*_(\\d{8})\\d*");

    private static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {
        Calendar calendar = Calendar.getInstance();

        calendar.setTimeInMillis(System.currentTimeMillis());

        calendar.add(Calendar.DAY_OF_YEAR, -7);

        Date endline = calendar.getTime();

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        String result = "/user/hdfs/result";

        FileStatus[] statuses = fs.listStatus(new Path(result));
        if (ArrayUtils.isEmpty(statuses)) {
            LOGGER.warn("statuses is empty");

            return;
        }

        Path[] datasets = FileUtil.stat2Paths(statuses);

        for (Path dataset : datasets) {
            String datasetName = dataset.getName();
            if (datasetName.equals("selectjob")) {
                continue;
            }

            statuses = fs.listStatus(dataset);
            if (ArrayUtils.isEmpty(statuses)) {
                LOGGER.warn("Path " + datasetName + " has no dirs, remove");

                fs.delete(dataset, true);

                continue;
            }

            Path[] jobnames = FileUtil.stat2Paths(statuses);

            for (Path jobname : jobnames) {
                statuses = fs.listStatus(jobname);
                if (ArrayUtils.isEmpty(statuses)) {
                    LOGGER.warn("Path " + jobname + " has no dirs, remove");

                    fs.delete(jobname, true);

                    continue;
                }

                Path[] jobrecords = FileUtil.stat2Paths(statuses);

                for (Path jobrecord : jobrecords) {
                    String jobrecordName = jobrecord.getName();

                    Matcher matcher = PATTERN.matcher(jobrecordName);

                    if (matcher.matches()) {
                        Date jobrecordDate = DAY_FORMAT.parse(matcher.group(1));

                        if (jobrecordDate.compareTo(endline) < 0) {
                            fs.delete(jobrecord, true);

                            LOGGER.info("Path " + jobrecord + " expired, remove success");
                        }
                    }
                }
            }
        }
    }

}
