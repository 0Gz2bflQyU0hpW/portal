package com.weibo.dip.data.platform.datacubic.link;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.streaming.util.KafkaProducerFactory;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
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
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;


/**
 * Created by delia on 2017/2/23.
 */
public class LinkLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkLog.class);
    private static String getSql() throws Exception {
        return String.join("\n", IOUtils.readLines(LinkLog.class.getClassLoader().getResourceAsStream("link.sql"), CharEncoding.UTF_8));
    }


    private static StructType getSchema(String[] columns){
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : columns) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext.sc()).enableHiveSupport().getOrCreate();

        String schemaString = "act subtype parseTime feed_type result_code during_time network_type time groupid netDataCount request_url net_time load_type ip ua from networktype uid";
        String[] columns = schemaString.split(" ");
        StructType schema = getSchema(columns);

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));

        Map<String, Integer> topics = new HashMap<>();
        topics.put("link", 1);
        int receivers = 1;
        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(receivers);

        for (int index = 0; index < receivers; index++) {
            kafkaStreams.add(KafkaUtils.createStream(streamingContext, "10.210.136.34:2181", "linkGroup", topics));
        }

        JavaPairDStream<String, String> unionStream = streamingContext.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        JavaDStream<String> sourceStream = unionStream.map(Tuple2::_2).map(String::trim);

        sourceStream.foreachRDD(lines->{
            LOGGER.info("foreachRDD");
            JavaRDD<Row> rowRDD = lines.map(new Function<String, Row>() {
                @Override
                public Row call(String record) throws Exception {
                    LOGGER.info("line: " + record);

                    String[] values = null;
                    Map<String, Object> pairs = null;

                    try {
                        pairs = GsonUtil.fromJson(record, GsonUtil.GsonType.OBJECT_MAP_TYPE);
                    } catch (Exception e) {}

                    if (MapUtils.isNotEmpty(pairs)) {
                        values = new String[columns.length];
                        for (int index = 0; index < columns.length; index++) {
                            Object value = pairs.get(columns[index]);

                            values[index] = (value != null) ? String.valueOf(value) : null;
                        }
                    }
                    LOGGER.info("data: "+ Arrays.toString(values));

                    return values != null ? RowFactory.create(values) : null;
                }
            }).filter(row -> {
                boolean isNonNull = Objects.nonNull(row);
                return isNonNull;
            });

            sparkSession.createDataFrame(rowRDD, schema).createOrReplaceTempView("fulllink");
            LOGGER.info("table");

            Dataset<Row> results = sparkSession.sql(getSql());
            List<Row> rows = results.javaRDD().collect();
            LOGGER.info("size: "+rows.size());
            Producer<String, String> producer = KafkaProducerFactory.getProducer("10.210.136.34:9092,10.210.136.80:9092,10.210.77.15:9092");
            LOGGER.info("producer");

            Map<String, Object> pairs = new HashMap<>();
            for (Row row : rows){
                LOGGER.info("results" + row.toString());

                pairs.clear();

                    String[] fieldNames = row.schema().fieldNames();

                    for (int index = 0; index < fieldNames.length; index++) {
                        pairs.put(fieldNames[index], row.get(index));
                    }
                    LOGGER.info("pairs");

                    String line = GsonUtil.toJson(pairs);
                    LOGGER.info("result: " + line);
                    producer.send(new ProducerRecord<>("linkres", line));

            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();


    }
}
