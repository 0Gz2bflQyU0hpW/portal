package com.weibo.dip.data.platform.datacubic.business;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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

/**
 * Created by yurun on 17/5/11.
 */
public class VideoNumberDaily {

    private static final Logger LOGGER = LoggerFactory.getLogger(VideoNumberDaily.class);

    private static String getSQL() throws Exception {
        StringBuilder sql = new StringBuilder();

        BufferedReader reader = null;

        try {
            reader = new BufferedReader(
                new InputStreamReader(
                    Ideo_Download_Base2slow.class.getClassLoader()
                        .getResourceAsStream("videonumberdaily.sql"),
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

        String inputPath = "/user/hdfs/result/selectjob/600/video_num_20180604151508/v1," +
            "/user/hdfs/result/selectjob/600/video_num_20180604142456/v1," +
            "/user/hdfs/result/selectjob/600/video_num_20180604134405/v1," +
            "/user/hdfs/result/selectjob/600/video_num_20180604120924/v1," +
            "/user/hdfs/result/selectjob/600/video_num_20180604110539/v1," +
            "/user/hdfs/result/selectjob/600/video_num_20180604102510/v1";

        LOGGER.info("inputPath: " + inputPath);

        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().enableHiveSupport().getOrCreate();

        String[] fieldNames = {"video_mediaid",
            "nu"};

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

        sourceDS.createOrReplaceTempView("source_table");

        Dataset<Row> resultDS = session.sql(getSQL());

        JavaRDD<String> lineRDD = resultDS.javaRDD().map(new Function<Row, String>() {

            @Override
            public String call(Row row) throws Exception {
                Object[] words = new Object[row.size()];

                for (int index = 0; index < row.size(); index++) {
                    words[index] = String.valueOf(row.get(index));
                    System.out.println("===========================" + String.valueOf(row.get(index)));
                }

                return StringUtils.join(words, "\t");
            }

        });

        lineRDD.repartition(1).saveAsTextFile("/tmp/videonumberdaily");

        context.stop();
    }

}
