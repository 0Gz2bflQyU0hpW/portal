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
import java.util.regex.Pattern;

public class CDNClientPic {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDNClientPic.class);

    private static String getPath(String[] args) {
        return ArrayUtils.isNotEmpty(args) ? "/user/hdfs/rawlog/app_picserversweibof6vwt_wapdownload/" + args[0] :
                "/user/hdfs/rawlog/app_picserversweibof6vwt_wapdownload/2018_03_19/00/app_picserversweibof6vwt_wapdownload-yf234092.scribe.dip.sina.com.cn_3738-2018_03_19_00-20180319004_00001";
    }

    private static final String regex = "(((https|http)?://)?([a-z0-9]+[.])|(www.))"
            + "\\w+[.|\\/]([a-z0-9]{0,})?[[.]([a-z0-9]{0,})]+((/[\\S&&[^,;\u4E00-\u9FA5]]+)+)?([.][a-z0-9]{0,}+|/?)";

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
            if(line.contains("|")) {
               Map<String, Object> lrs = GsonUtil.fromJson(line.substring(line.indexOf("|") + 1,line.length()), GsonUtil.GsonType.OBJECT_MAP_TYPE);
               if(lrs.containsKey("pic_url") && lrs.get("pic_url") != null){
                   if(Pattern.compile(regex.trim()).matcher((String) lrs.get("pic_url")).matches()){
                       matchCount.add(1L);
                       return false;
                   }else{
                       return true;
                   }
               }else{
                   return true;
               }
            }else{
                return true;
            }
        });

        lines_result.repartition(1).saveAsTextFile("/tmp/cdn_client_pic");

        System.out.println("srcCount : " + srcCount.value() + " matchCount : " + matchCount.value());

        context.stop();

    }
}
