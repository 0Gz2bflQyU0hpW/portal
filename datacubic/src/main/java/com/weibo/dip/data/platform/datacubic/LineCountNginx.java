package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.datacubic.batch.BatchProcess;
import com.weibo.dip.data.platform.datacubic.batch.util.BatchUtils;
import jodd.util.ArraysUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.cmd.Spec;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by delia on 2017/1/11.
 */
public class LineCountNginx {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineCountNginx.class);

    private static final String path = "/user/hdfs/rawlog/www_sinaedgeahsolci14ydn_nginx/";

    public static void main(String[] args) throws Exception {
        if (ArrayUtils.isEmpty(args) || args.length != 2){
            LOGGER.info("input YYYYMMDD, ip");
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

        String ip = args[1];

        String info = String.format("path: %s;%s;%s,ip:%s, time %s",HDFS_PATH_YES, HDFS_PATH_Tod, HDFS_PATH_Tom, ip, contengDate);

        System.out.println(info);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());

        JavaRDD<String> lines_28 = sc.textFile(HDFS_PATH_YES);
        JavaRDD<String> lines_29 = sc.textFile(HDFS_PATH_Tod);
        JavaRDD<String> lines_30 = sc.textFile(HDFS_PATH_Tom);

        LongAccumulator srcCount = sc.sc().longAccumulator();
        LongAccumulator matchCount = sc.sc().longAccumulator();

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

                if (!sip.equals(ip) || (createtime.indexOf(contengDate) == -1)) {
                    return false;
                }

                matchCount.add(1L);
                return true;
            }

            return false;
        });
        // time filter
        long num = lines.count();
        System.out.println("srcCount : " + srcCount.value() + " matchCount : " + matchCount.value() + "line num = " + num);
        sc.stop();

    }
}
