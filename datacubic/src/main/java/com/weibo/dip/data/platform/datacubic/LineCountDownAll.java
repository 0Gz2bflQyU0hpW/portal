package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by delia on 2017/1/11.
 */
public class LineCountDownAll {
    private static String HDFS_PATH_9 = "/user/hdfs/rawlog/app_picserversweibof6vwt_wapvideodownload/2017_01_15/*";
    private static String HDFS_PATH_8 = "/user/hdfs/rawlog/app_picserversweibof6vwt_wapvideodownload/2017_01_14/23/*";
    private static String HDFS_PATH_10 = "/user/hdfs/rawlog/app_picserversweibof6vwt_wapvideodownload/2017_01_16/00/*";
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    private static SimpleDateFormat format_YMD = new SimpleDateFormat("yyyy_MM_dd");


    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines_9 = sc.textFile(HDFS_PATH_9);
        JavaRDD<String> lines_8 = sc.textFile(HDFS_PATH_8);
        JavaRDD<String> lines_10 = sc.textFile(HDFS_PATH_10);

        JavaRDD<String> lines = lines_9.union(lines_8).union(lines_10).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                // time filter
                if (StringUtils.isEmpty(v1)) {
                    return false;
                }

                String[] res = v1.split("`");

                if (res.length < 2) {
                    return false;
                }

                Map<String, Object> keyValues = null;

                try {
                    keyValues = GsonUtil.fromJson(res[0], GsonUtil.GsonType.OBJECT_MAP_TYPE);
                } catch (Exception e) {
                    return false;
                }

                if (keyValues == null || !keyValues.containsKey("@timestamp")) {
                    return false;
                }

                String timestamp = (String) keyValues.get("@timestamp");

                if (timestamp == null) {
                    return false;
                }

                Date date = null;
                try {
                    date = format.parse(timestamp);
                } catch (Exception e) {
                    return false;
                }

                if (date == null) {
                    return false;
                }

                String sDate = format_YMD.format(date);
                if (!sDate.equals("2017_01_15")) {
                    return false;
                }

                return true;
            }
        });
        System.out.println("line num = " + lines.count());
        sc.stop();

    }
}
