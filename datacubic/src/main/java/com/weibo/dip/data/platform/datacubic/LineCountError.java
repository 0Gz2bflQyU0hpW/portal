package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.youku.entity.Result;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by delia on 2017/1/11.
 */
public class LineCountError {
    private static final String path = "/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_trafficserver/";
//    private static String HDFS_PATH_11 = path + "2017_05_11/*";
//    private static String HDFS_PATH_10 = path + "2017_05_10/23/*";
//    private static String HDFS_PATH_12 = path + "2017_05_12/00/*";
    private static String HDFS_PATH_17 = path + "2017_05_17/*";

    public static void main(String[] args) {
//        System.out.println("path : " + HDFS_PATH_11);
//        System.out.println("path : " + HDFS_PATH_10);
//        System.out.println("path : " + HDFS_PATH_12);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
//        JavaRDD<String> lines_11 = sc.textFile(HDFS_PATH_11);
//        JavaRDD<String> lines_12 = sc.textFile(HDFS_PATH_12);
//        JavaRDD<String> lines_10 = sc.textFile(HDFS_PATH_10);
        JavaRDD<String> lines_17 = sc.textFile(HDFS_PATH_17);

        Pattern pattern = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

        JavaRDD<String> lines = lines_17.map(line -> {
            if (StringUtils.isEmpty(line)) {
                return null;
            }

            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                int count = matcher.groupCount();
                if (count != 18) {
                    return null;
                }
                String sip = matcher.group(2);
                String createtime = matcher.group(9);

                if (StringUtils.isEmpty(sip)) {
                    return null;
                }

                return sip;
            }

            return null;
        }).filter(StringUtils::isNotEmpty).distinct();

        lines.repartition(1).saveAsTextFile("/tmp/ip");

        sc.stop();

    }
}
