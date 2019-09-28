package com.weibo.dip.data.platform.datacubic.streaming;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * Created by xiaoyu on 2017/2/23.
 */
public class MapErrorDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapErrorDetector.class);

    private static final String HDFS_INPUT = "/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance";

    private static final String HDFS_OUTPUT = "/tmp/fulllink";

    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    public static String getInputPath(Date today, int hour) {
        return HDFS_INPUT + "/" + YYYY_MM_DD.format(today) + "/" + String.format("%02d", hour) + "/*";
    }

    public static String getOutputPath(Date today, int hour) {
        return HDFS_OUTPUT + "/" + YYYY_MM_DD.format(today) + "/" + String.format("%02d", hour);
    }

    public static void main(String[] args) throws Exception {
        Date now = new Date();

        String inputPath = getInputPath(now, 4);
        String outputPath = getOutputPath(now, 4);

        LOGGER.info("input path: " + inputPath);
        LOGGER.info("output path: " + outputPath);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines = sc.textFile(inputPath).map(line -> {
            Map<String, Object> pairs;

            try {
                pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
            } catch (Exception e) {
                LOGGER.warn("parse line " + line + " error: " + ExceptionUtils.getFullStackTrace(e));

                return null;
            }

            if (MapUtils.isEmpty(pairs)
                || (pairs.get("act") == null) || (!pairs.get("act").equals("performance"))
                || (pairs.get("subtype") == null) || (!pairs.get("subtype").equals("refresh_feed"))) {
                return null;
            }

            String ua = (String) pairs.get("ua");
            if (StringUtils.isEmpty(ua)) {
                return line;
            }

            String allParams[] = ua.split("__", -1);
            if (ua.split("__", -1).length != 5) {
                return line;
            }

            int index = allParams[0].indexOf("-");
            if (index < 0){
                return line;
            }

            return null;
        }).filter(Objects::nonNull);

        lines.saveAsTextFile(outputPath);

        sc.close();

    }

}
