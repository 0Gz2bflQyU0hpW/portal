package com.weibo.dip.data.platform.datacubic.fulllink.sla;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/7/11.
 */
public class UniqueUIDs {

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("^.*\"uid\":\"(\\d+)\".*$");

        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = context.textFile(args[0]);

        List<String> uids = lineRDD
            .mapToPair((PairFunction<String, String, Long>) line -> {
                Matcher matcher = pattern.matcher(line);

                if (matcher.matches()) {
                    String uid = matcher.group(1);

                    if (uid.charAt(uid.length() - 3) == '0' && uid.charAt(uid.length() - 2) == '5') {
                        return new Tuple2<>(uid, 1L);
                    }
                }

                return new Tuple2<>(null, 0L);
            })
            .filter(record -> Objects.nonNull(record._1()))
            .reduceByKey((a, b) -> a + b)
            .mapToPair(record -> new Tuple2<>(record._2(), record._1()))
            .sortByKey()
            .map(record -> record._2())
            .top(1000);

        context.stop();

        if (CollectionUtils.isNotEmpty(uids)) {
            uids.stream().forEach(System.out::println);
        }
    }

}
