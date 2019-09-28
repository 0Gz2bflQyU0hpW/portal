package com.weibo.dip.data.platform.datacubic.youku;

import com.weibo.dip.data.platform.datacubic.youku.entity.PlayLog;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetDomainId;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetHour;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetPlatform;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetVideoId;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author delia
 */

public class PlayLogIOSStatistic {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlayLogIOSStatistic.class);

    private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    private static String getYesterday(Date today, SimpleDateFormat format) {
        Calendar cal = Calendar.getInstance();

        cal.setTime(today);

        cal.add(Calendar.DAY_OF_MONTH, -1);

        return format.format(cal.getTime());
    }

    private static String getInputPath(Date today) {
        String datasetDir = "/user/hdfs/rawlog/app_weibomobilekafka1234_weibomobileaction799";

        String yesterdayDir = getYesterday(today, YYYY_MM_DD);

        return datasetDir + "/" + yesterdayDir + "/00" + "/*";
    }

    private static String getSQL() throws Exception {
        return String.join("\n", IOUtils.readLines(PlayLogIOSStatistic.class.getClassLoader().getResourceAsStream("youku_play_ios.sql"), CharEncoding.UTF_8));
    }

    private static String rowToLine(Row row) {
        List<String> columns = new ArrayList<>(row.length());

        for (int index = 0; index < row.length(); index++) {
            columns.add(row.getString(index));
        }

        return String.join("\t", columns);
    }

    private static String getOutputPath(Date today) {
        return "/tmp/youku/" + System.currentTimeMillis();
    }

    public static void main(String[] args) {
        try {
            Date today = new Date();

            SparkSession sparkSession = SparkSession.builder().getOrCreate();

            JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

            JavaRDD<String> srcRdd = sparkContext.textFile(getInputPath(today));

            JavaRDD<PlayLog> desDataset = srcRdd.map(PlayLog::build).filter(playLog -> playLog != null);

            sparkSession.createDataFrame(desDataset, PlayLog.class).createOrReplaceTempView("logdata");

            sparkSession.udf().register("getVideoId", new GetVideoId(), DataTypes.StringType);
            sparkSession.udf().register("getDomainId", new GetDomainId(), DataTypes.StringType);
            sparkSession.udf().register("getPlatform", new GetPlatform(), DataTypes.StringType);
            sparkSession.udf().register("getHour", new GetHour(getYesterday(today, YYYYMMDD)), DataTypes.StringType);

            Dataset<Row> result = sparkSession.sql(getSQL());

            result.javaRDD().map(PlayLogIOSStatistic::rowToLine).repartition(1).saveAsTextFile(getOutputPath(today));

            sparkContext.close();

            sparkSession.stop();
        } catch (Exception e) {
            LOGGER.error("Execute error: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

}



