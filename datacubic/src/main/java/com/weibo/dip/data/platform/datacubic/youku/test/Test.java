package com.weibo.dip.data.platform.datacubic.youku.test;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author delia
 * @create 2016-12-09 上午11:13
 */

public class Test {

    public static void main(String[] args) {
        SparkConf conf  = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1,2,3,3);
        JavaRDD<Integer> rdd = context.parallelize(list);
        Integer res = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        JavaPairRDD<String,Integer> pair = rdd.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer integer) throws Exception {
                return new Tuple2<String,Integer>(integer.toString(),integer);
            }
        });
        JavaPairRDD<String,Integer> pairNew = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        pairNew.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2._1+" "+ stringIntegerTuple2._2.toString());
            }
        });

    }
}



