package com.weibo.dip.data.platform.datacubic.business;


import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CDNClientVideoCheck {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDNClientVideoCheck.class);

    private static String getPath(String[] args) {
        return ArrayUtils.isNotEmpty(args) ? "/user/hdfs/rawlog/app_picserversweibof6vwt_wapdownload/" + args[0] :
                "/user/hdfs/rawlog/app_picserversweibof6vwt_wapvideodownload/2018_03_19/00/*";
    }

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile(getPath(args));

        LongAccumulator srcCount = context.sc().longAccumulator();
        LongAccumulator matchCount = context.sc().longAccumulator();

        JavaRDD<String> lines_result = lines.filter(line -> {
            srcCount.add(1L);
            if (StringUtils.isEmpty(line)) {
                return false;
            }
            if(line.split("`").length == 2 && line.split("`")[0] != null) {
                Map<String, Object> lrs = GsonUtil.fromJson(line.split("`")[0], GsonUtil.GsonType.OBJECT_MAP_TYPE);
               if(lrs.containsKey("video_cdn")){
                       matchCount.add(1L);
                       return true;
               }else{
                   return false;
               }
            }else{
                return false;
            }
        });

        lines_result.repartition(1).saveAsTextFile("/tmp/cdn_client_video_check");

        System.out.println("srcCount : " + srcCount.value() + " matchCount : " + matchCount.value());

        context.stop();

    }
}
