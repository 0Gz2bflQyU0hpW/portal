package com.weibo.dip.data.platform.datacubic;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by yurun on 17/4/12.
 */
public class SparkSQLMain {

    public static class ReturnArrayUDF implements UDF1<String, List<String>> {

        @Override
        public List<String> call(String value) throws Exception {
            return Arrays.asList(value, value, value);
        }

    }

    public static class ReturnMapUDF implements UDF1<String, Map<String, String>> {

        @Override
        public Map<String, String> call(String value) throws Exception {
            return Collections.singletonMap("value", value);
        }

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setAppName("spark_sql");
        conf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(conf);

        SparkSession session = SparkSession.builder().sparkContext(context.sc()).enableHiveSupport().getOrCreate();

        session.udf().register("return_array", new ReturnArrayUDF(), DataTypes.createArrayType(DataTypes.StringType));

        List<String> lines = new ArrayList<>();

        lines.add("1 2 3");
        lines.add("4 5 6");
        lines.add("7 8 9");

        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("first", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("second", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("third", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rows = context.parallelize(lines).map(line -> {
            String[] words = line.split(" ");

            return RowFactory.create(words[0], words[1], words[2]);
        });

        Dataset<Row> dataset = session.createDataFrame(rows, schema);

        dataset.createOrReplaceTempView("mytable");

        Dataset<Row> results = session.sql("select return_array(first) from mytable");

        List<Row> datas = results.javaRDD().collect();

        context.close();

        for (Row data : datas) {
            StructType columnSchema = data.schema();

            System.out.println(columnSchema.fields()[0].dataType() instanceof ArrayType);

            Map<String, Object> map = new HashMap<>();

            map.put("key", data.getList(0));

            System.out.println(GsonUtil.toJson(map));
        }
    }

}
