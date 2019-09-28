package com.weibo.dip.data.platform.datacubic.batch;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by delia on 2017/3/28.
 */
public class BatchProcess {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchProcess.class);
    private static final String DATA_SET_NAME = "";

    private static final Pattern PATTERN = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");


    public static void main(String[] args) {
        if (ArrayUtils.isEmpty(args) || args.length != 1) {
            LOGGER.info("must input ip.");
            return;
        }
        String datasetName = args[0];
        String inputPath = "/user/hdfs/rawlog/" + datasetName + "/2017_03_27/*";
        String inputPath1 = "/user/hdfs/rawlog/" + datasetName + "/2017_03_26/23/*";
        String inputPath2 = "/user/hdfs/rawlog/" + datasetName + "/2017_03_28/00/*";

        LOGGER.info(String.format("datasets: %s,%s,%s", inputPath, inputPath1, inputPath2));

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = context.textFile(inputPath);
        JavaRDD<String> rdd1 = context.textFile(inputPath1);
        JavaRDD<String> rdd2 = context.textFile(inputPath2);
        JavaRDD<String> rddSrc = rdd.union(rdd1).union(rdd2);

        LongAccumulator sourceCount = context.sc().longAccumulator();
        LongAccumulator matchCount = context.sc().longAccumulator();
        LongAccumulator disMatchCount = context.sc().longAccumulator();
        LongAccumulator invalidCount = context.sc().longAccumulator();

        JavaRDD<String> resRDD = rddSrc.filter(line -> {
            sourceCount.add(1L);
            Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches()) {
                String sip = matcher.group(2).trim();
                String time = matcher.group(9);
                if (time.contains("27/Mar/2017") && sip.equals("222.76.214.35")) {
                    matchCount.add(1L);
                    return true;
                } else {
                    disMatchCount.add(1L);
                    return false;
                }
            }
            invalidCount.add(1L);
            return false;
        });

        long resultNum = resRDD.count();
        LOGGER.info(datasetName + ": source count = " + sourceCount.value() + ",invalid count = " + invalidCount.value() + ",match count = " + matchCount.value() + ",dismatch count = " + disMatchCount.value() + ",result count = " + resultNum);

        context.stop();
    }

}
