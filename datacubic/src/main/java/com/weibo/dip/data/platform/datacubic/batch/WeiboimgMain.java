package com.weibo.dip.data.platform.datacubic.batch;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/3/23.
 */
public class WeiboimgMain {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_weiboimgerr/2017_03_22/*");

        Pattern pattern = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

        LongAccumulator input = context.sc().longAccumulator("input");
        LongAccumulator match = context.sc().longAccumulator("match");
        LongAccumulator output = context.sc().longAccumulator("output");

        lines.filter(line -> {
            input.add(1L);

            if (StringUtils.isEmpty(line)) {
                return false;
            }

            Matcher matcher = pattern.matcher(line);

            if (!matcher.matches()) {
                return false;
            }

            match.add(1L);

            String url = matcher.group(12);
            String httpcode = matcher.group(14);

            if (url.contains("http://n.sinaimg.cn/ent/crawl/20170322/wFuY-fycnyhm1634491.jpg") && httpcode.equals("404")) {
                output.add(1L);

                return true;
            }

            return false;
        }).repartition(1).saveAsTextFile("/tmp/weiboimg/");

        context.stop();

        System.out.println("input: " + input.value() + ", match: " + match.value() + ", output: " + output.value());
    }

}
