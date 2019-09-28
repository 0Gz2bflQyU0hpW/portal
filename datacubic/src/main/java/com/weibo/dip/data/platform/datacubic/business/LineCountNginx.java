package com.weibo.dip.data.platform.datacubic.business;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LineCountNginx {
    private static final String path = "/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_nginx/2017_12_17/*";

    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines_1217 = sc.textFile(path);

        LongAccumulator srcCount = sc.sc().longAccumulator();
        LongAccumulator matchCount = sc.sc().longAccumulator();

        Pattern pattern = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

        JavaRDD<String> lines = lines_1217.filter(line -> {
            srcCount.add(1L);

            if (StringUtils.isEmpty(line)) {
                return false;
            }

            Matcher matcher = pattern.matcher(line);
            if (matcher.matches() ) {
                String domain = matcher.group(5);
                if(domain.endsWith("us.sinaimg.cn")){
                    System.out.println(domain);
                    matchCount.add(1L);
                    return true;
                }
                return false;
            }

            return false;
        });
        // time filter
        lines.repartition(10).saveAsTextFile("/tmp/nginx/");
        System.out.println("srcCount : " + srcCount.value() + " matchCount : " + matchCount.value());
        sc.stop();

    }
}
