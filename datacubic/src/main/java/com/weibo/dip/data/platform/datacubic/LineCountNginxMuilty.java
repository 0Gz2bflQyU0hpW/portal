package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.datacubic.batch.util.BatchUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by delia on 2017/1/11.
 */
public class LineCountNginxMuilty {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineCountNginxMuilty.class);

    private static final String path = "/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_nginx/";

    public static void main(String[] args) throws Exception {
        if (ArrayUtils.isEmpty(args) || args.length != 1) {
            LOGGER.info("input YYYYMMDD");
            return;
        }

        String time = args[0];

        if (!BatchUtils.isYYYYMMDD(time)) {
            LOGGER.error("input time digital like YYYYMMDD!");
            return;
        }

        SimpleDateFormat formatTime = new SimpleDateFormat("yyyyMMdd");
        Date date = formatTime.parse(time);

        String pathYes = BatchUtils.getYesterDay_YY_MM_DD(date);
        String pathTod = BatchUtils.getToday_YY_MM_DD(date);
        String pathTom = BatchUtils.getTomorrow_YY_MM_DD(date);

        String HDFS_PATH_YES = path + pathYes + "/23/*";
        String HDFS_PATH_Tod = path + pathTod + "/*";
        String HDFS_PATH_Tom = path + pathTom + "/00/*";

        SimpleDateFormat contentDateFormat = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);

        String contengDate = contentDateFormat.format(date);

        String ip1 = "112.13.174.77";
        String ip2 = "112.13.174.75";
        String ip3 = "112.13.174.76";
        String ip4 = "112.13.174.78";
        String ip5 = "112.13.174.79";

        String info = String.format("path: %s;%s;%s,time %s", HDFS_PATH_YES, HDFS_PATH_Tod, HDFS_PATH_Tom, contengDate);

        System.out.println(info);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines_28 = sc.textFile(HDFS_PATH_YES);
        JavaRDD<String> lines_29 = sc.textFile(HDFS_PATH_Tod);
        JavaRDD<String> lines_30 = sc.textFile(HDFS_PATH_Tom);

        LongAccumulator srcCount = sc.sc().longAccumulator();
        LongAccumulator matchCountip1 = sc.sc().longAccumulator();
        LongAccumulator matchCountip2 = sc.sc().longAccumulator();
        LongAccumulator matchCountip3 = sc.sc().longAccumulator();
        LongAccumulator matchCountip4 = sc.sc().longAccumulator();
        LongAccumulator matchCountip5 = sc.sc().longAccumulator();

        Pattern pattern = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

        JavaRDD<String> lines = lines_29.union(lines_30).union(lines_28).filter(line -> {
            srcCount.add(1L);

            if (StringUtils.isEmpty(line)) {
                return false;
            }

            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                int count = matcher.groupCount();
                if (count != 18) {
                    return false;
                }
                String sip = matcher.group(2).trim();
                String createtime = matcher.group(9);

                if (StringUtils.isEmpty(sip) || StringUtils.isEmpty(createtime)) {
                    return false;
                }

                if (createtime.indexOf(contengDate) == -1) {
                    return false;
                }
                if (sip.equals(ip1)) {
                    matchCountip1.add(1L);

                    return true;
                }

                if (sip.equals(ip2)) {
                    matchCountip2.add(1L);

                    return true;
                }

                if (sip.equals(ip3)) {
                    matchCountip3.add(1L);

                    return true;
                }

                if (sip.equals(ip4)) {
                    matchCountip4.add(1L);

                    return true;
                }

                if (sip.equals(ip5)) {
                    matchCountip5.add(1L);

                    return true;
                }

                return false;
            }

            return false;
        });

        long num = lines.count();

        String res = String.format("count: %s,srcCount : %s, %s Count: %s, %s Count: %s, %s Count: %s, %s Count: %s, %s Count: %s", num, srcCount.value(),
                ip1, matchCountip1.value(), ip2, matchCountip2.value(), ip3, matchCountip3.value(), ip4, matchCountip4.value(), ip5, matchCountip5.value());
        System.out.println(res);
        sc.stop();

    }
}
