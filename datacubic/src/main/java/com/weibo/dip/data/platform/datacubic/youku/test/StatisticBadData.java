package com.weibo.dip.data.platform.datacubic.youku.test;

import com.weibo.dip.data.platform.datacubic.youku.PlayLogStatistic;
import com.weibo.dip.data.platform.datacubic.youku.entity.PlayLog;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetDomainId;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetHour;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetPlatform;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetVideoId;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;

/**
 * Created by delia on 16/12/28.
 */
public class StatisticBadData {

    static SparkSession sparkSession = SparkSession.builder().getOrCreate();
    static JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
    private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

    private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

    private static final SimpleDateFormat YMD = new SimpleDateFormat("yyyyMMdd");


    public static void main(String[] args) throws Exception {
        String log_input_path = "file:///data0/xiaoyu/data/*";

            JavaRDD<String> srcRdd = sparkContext.textFile(log_input_path);

        JavaRDD<PlayLog> desDataset = srcRdd.map(PlayLog::build).filter(Objects::nonNull);

        sparkSession.createDataFrame(desDataset, PlayLog.class).createOrReplaceTempView("logdata");

        sparkSession.udf().register("getVideoId", new GetVideoId(), DataTypes.StringType);
        sparkSession.udf().register("getDomainId", new GetDomainId(), DataTypes.StringType);
        sparkSession.udf().register("getPlatform", new GetPlatform(), DataTypes.StringType);
        sparkSession.udf().register("getHour", new GetHour("2016-12-28"), DataTypes.StringType);

        String sql = getSQL();

        Dataset<Row> results = sparkSession.sql(sql);

        List<Row> rows = results.javaRDD().collect();

        for (Row row:rows){
            System.out.println(row.toString());
        }
    }

    private static String getSQL() throws Exception {
        return String.join("\n", IOUtils.readLines(PlayLogStatistic.class.getClassLoader().getResourceAsStream("youku_play_test.sql"), CharEncoding.UTF_8));
    }

}
