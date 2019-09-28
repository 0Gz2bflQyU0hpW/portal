package com.weibo.dip.data.platform.datacubic.batch.util;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchConstant;
import com.weibo.dip.data.platform.datacubic.streaming.Constants;
import com.weibo.dip.data.platform.datacubic.streaming.StreamingEngine;
import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;
import com.weibo.dip.data.platform.datacubic.youku.entity.Result;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoyu on 2017/6/12.
 */
public class BatchUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchUtils.class);

    private static SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    public static boolean isYYYYMMDD(String time) {
        Pattern patternDigital = Pattern.compile("^\\d{8}$");
        Matcher matcherDigital = patternDigital.matcher(time);
        if (!matcherDigital.matches()) {
            return false;
        }
        return true;
    }

    public static Date getSpecifiedDate(Date date, Integer step) {
        Calendar cal = Calendar.getInstance();

        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, step);
        return cal.getTime();
    }

    public static Date getYesterdayDate(Date date) {

        return getSpecifiedDate(date, -1);
    }

    public static Date getTomorrowDate(Date date) {

        return getSpecifiedDate(date, 1);
    }

    public static synchronized String getYesterDay_YY_MM_DD(Date date) {
        Date yesterday = getYesterdayDate(date);

        return YYYY_MM_DD.format(yesterday);
    }

    public static synchronized String getToday_YY_MM_DD(Date date) {
        return YYYY_MM_DD.format(date);
    }

    public static synchronized String getTomorrow_YY_MM_DD(Date date) {
        Date tomorrow = getTomorrowDate(date);

        return YYYY_MM_DD.format(tomorrow);
    }

    public static Date getTodayDate(String[] args) throws Exception {
        if (args.length == 2) {

            String time = args[1];

            if (!BatchUtils.isYYYYMMDD(time)) {
                LOGGER.error("input time digital like yyyyMMdd!");
                return null;
            }
            return new SimpleDateFormat("yyyyMMdd").parse(time);
        }
        return new Date();
    }

    public static String getInputPathYesterday(String inputPathRoot, Date date) {
        String yesterday = getYesterDay_YY_MM_DD(date);
        return inputPathRoot + "/" + yesterday + "/*";
    }

    public static String getInputPathYesterday(String inputPathRoot, String[] args) throws Exception {
        Date today = getTodayDate(args);
        String yesterday = getYesterDay_YY_MM_DD(today);
        return inputPathRoot + "/" + yesterday + "/*";
    }

    public static synchronized String getInputPathToday(String inputPathRoot, Date date) {
        String today = YYYY_MM_DD.format(date);

        return inputPathRoot + "/" + today + "/*";
    }

    public static synchronized String getInputPathToday(String inputPathRoot, String[] args) throws Exception {
        Date date = getTodayDate(args);
        String today = YYYY_MM_DD.format(date);

        return inputPathRoot + "/" + today + "/*";
    }

    public static Map<String, RowMapper> getRowMappers() throws Exception {
        Properties properties = new Properties();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(StreamingEngine.class.getClassLoader().getResourceAsStream(BatchConstant.MAPPERS_CONFIG), CharEncoding.UTF_8));

            properties.load(reader);
        } finally {
            IOUtils.closeQuietly(reader);
        }

        Map<String, RowMapper> mappers = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            String classImpl = properties.getProperty(name);

            mappers.put(name, (RowMapper) Class.forName(classImpl).newInstance());
        }

        properties.clear();

        return mappers;
    }

    public static Producer<String, String> getProducer(String servers){
        Map<String, Object> config = new HashMap<>();

        config.put("bootstrap.servers", servers);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(config);
    }
    public static long writeToHdfs( Iterable<Row> rows, String path) throws Exception{
        Path outputPath = new Path(path);

        long num = 0;

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);

            LOGGER.info("outputPath " + outputPath + " already exists, deleted");
        }

            LOGGER.info("write data to " + path);

            BufferedWriter writer = null;

            try {
                writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath), CharEncoding.UTF_8));

                for (Row row : rows) {
                    String line = row.getString(0) + "    " + row.getLong(1);
                    writer.write(line);
                    writer.newLine();
                    num ++;
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }

            return num;
    }

    public static void main(String[] args) {
        Date today = new Date();

//        System.out.println(getYesterDay_YY_MM_DD(today));
//        System.out.println(getYYMMDDTomorrow(today));

        SimpleDateFormat contentDate = new SimpleDateFormat("dd/MMM/yyyy", Locale.ENGLISH);
//        SimpleDateFormat contentDate = new SimpleDateFormat("dd/mmm/yyyy", Locale.ENGLISH);
        System.out.println(contentDate.format(today));

    }
}
