package com.weibo.dip.data.platform.datacubic.streaming;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by delia on 2017/2/22.
 */
public class FilterLog {

    private static String INPUT_PATH = "/user/hdfs/rawlog/link/";
    private static String OUTPUT_PATH = "/tmp/link/";

    public static void main(String[] args) throws IOException {
        JavaSparkContext context = new JavaSparkContext(new SparkConf());
        JavaRDD<String> srcRdd = context.textFile(INPUT_PATH).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                Map<String, Object> pairs = null;
                try {
                    pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
                } catch (Exception e) {
                }

                if (MapUtils.isNotEmpty(pairs)){
                    Object act = pairs.get("act");
                    Object type = pairs.get("subtype");
                    if (act == null || type == null || !String.valueOf(act).equals("performance") || !(String.valueOf(type).equals("refresh_feed"))){
                        return false;
                    }
                }
                return true;
            }
        });
        srcRdd.saveAsTextFile("/tmp/aaa/srcRdd");
        context.stop();

    }

    public static void writeout(JavaRDD<String> srcRdd) throws IOException {
        Path outputPath = new Path(OUTPUT_PATH,"srcRdd");
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("outputPath " + outputPath + " already exists, deleted");
        }
        BufferedWriter writer = null;


    }
}
