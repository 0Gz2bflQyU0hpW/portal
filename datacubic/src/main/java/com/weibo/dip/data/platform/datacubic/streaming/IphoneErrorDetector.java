package com.weibo.dip.data.platform.datacubic.streaming;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.collections.MapUtils;
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
public class IphoneErrorDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(IphoneErrorDetector.class);

    private static final String HDFS_INPUT = "/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance/2017_03_01/04";

    private static final String HDFS_OUTPUT = "/tmp/fulllink/2017_03_01/04";

    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    public static void main(String[] args) throws Exception {
        Date now = new Date();

        LOGGER.info("input path: " + HDFS_INPUT);
        LOGGER.info("output path: " + HDFS_OUTPUT);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines = sc.textFile(HDFS_INPUT).map(line -> {
            Map<String, Object> pairs;

            try {
                pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
            } catch (Exception e) {
                LOGGER.warn("parse line " + line + " error: " + ExceptionUtils.getFullStackTrace(e));

                return null;
            }

            if (MapUtils.isEmpty(pairs) || (pairs.get("subtype") == null) || (!pairs.get("subtype").equals("refresh_feed"))) {
                return null;
            }

            if (pairs.get("ua") == null) {
                return "no ua: " + line;
            }
            if ((pairs.get("time") == null)) {
                return "no time: " + line;
            }
            if ((pairs.get("result_code") == null)) {
                return "no result_code: " + line;
            }
            if (pairs.get("during_time") == null) {
                return "no during_time: " + line;
            }
            if (pairs.get("net_time") == null) {
                return "no net_time: " + line;
            }

            return null;
        }).filter(Objects::nonNull);

        lines.saveAsTextFile(HDFS_OUTPUT);

        sc.close();
    }

}
