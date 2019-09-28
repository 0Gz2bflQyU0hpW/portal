package com.weibo.dip.data.platform.datacubic.business;

import com.weibo.dip.data.platform.datacubic.streaming.udf.IpToLocation;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

//import javafx.scene.text.Text;

public class Ideo_Download_Base2slow {
    private static final Logger LOGGER = LoggerFactory.getLogger(Ideo_Download_Base2slow.class);

    private static String getSQL() throws Exception {
        StringBuilder sql = new StringBuilder();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(
                    new InputStreamReader(
                            Ideo_Download_Base2slow.class.getClassLoader()
                                    .getResourceAsStream("Ideo_Download_Base2slow.sql"),
                            CharEncoding.UTF_8));

            String line;

            while ((line = reader.readLine()) != null) {
                sql.append(line);
                sql.append("\n");
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return sql.toString();
    }

    public static void main(String[] args) throws Exception {

        String inputPath = "/user/hdfs/result/selectjob/512/video_download_base_2017121601/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121602/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121603/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121604/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121605/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121606/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121607/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121608/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121609/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121610/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121611/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121612/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121613/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121614/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121615/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121616/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121617/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121618/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121619/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121620/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121621/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121622/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121623/baseData,"+
                "/user/hdfs/result/selectjob/512/video_download_base_2017121700/baseData";

        LOGGER.info("inputPath: " + inputPath);

        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

        session.udf().register("ipToLocation", new IpToLocation(), DataTypes.createMapType(DataTypes
                .StringType, DataTypes.StringType));

        String[] fieldNames = {"ip",
                "version",
                "error_code",
                "cdn",
                "ua",
                "ap",
                "video_firstframe_status",
                "video_quit_status",
                "video_firstframe_time",
                "video_buffering_count",
                "video_duration_timesum",
                "video_valid_play_duration",
                "video_download_size",
                "video_player_type",
                "video_type",
                "video_buffering_duration",
                "video_url"};

        List<StructField> fields = new ArrayList<>();

        for (String fieldName : fieldNames) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }

        StructType schema = DataTypes.createStructType(fields);

        JobConf jobConf = new JobConf();
        jobConf.set(FileInputFormat.INPUT_DIR, inputPath);
        jobConf.setBoolean(FileInputFormat.INPUT_DIR_RECURSIVE, true);

        JavaPairRDD<LongWritable, Text> javaPairRDD = context.hadoopRDD(jobConf, TextInputFormat.class, LongWritable.class, Text.class);

        @SuppressWarnings("unchecked")
        JavaRDD<String> sourceRDD = javaPairRDD.map(new Function<Tuple2<LongWritable, Text>, String>() {
            @Override
            public String call(Tuple2<LongWritable, Text> tuple2) {
                return tuple2._2().toString();
            }
        });

        JavaRDD<Row> rowRDD = sourceRDD.map(line -> {

            String[] row = line.split("\t", -1);

            String[] values = new String[fieldNames.length];

            for (int index = 0; index < fieldNames.length; index++) {

                values[index] = row[index];
            }

            return RowFactory.create((Object[]) values);
        }).filter(Objects::nonNull);

        Dataset<Row> sourceDS = session.createDataFrame(rowRDD, schema);

        sourceDS.createOrReplaceTempView("download_slow");

        Dataset<Row> resultDS = session.sql(getSQL());

        JavaRDD<String> lineRDD = resultDS.javaRDD().map(new Function<Row, String>() {

            @Override
            public String call(Row row) throws Exception {
                Object[] words = new Object[row.size()];

                for (int index = 0; index < row.size(); index++) {
                    words[index] = String.valueOf(row.get(index));
                }

                return StringUtils.join(words, "\t");
            }

        });

        lineRDD.repartition(1).saveAsTextFile("/tmp/ideo_download_v1_1217");

        context.stop();
    }
}
