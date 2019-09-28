package com.weibo.dip.data.platform.datacubic.batch;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** @author yurun */
public class SparkSQLMain {
  public static class MyUdf implements UDF1<String, String> {
    private String seed;

    public MyUdf(String seed) {
      this.seed = seed;
    }

    @Override
    public String call(String str) throws Exception {
      return str.toUpperCase() + seed;
    }
  }

  public static void main(String[] args) {
    SparkConf conf = new SparkConf();

    conf.setAppName("spark_sql_main");
    conf.setMaster("local[3]");

    JavaSparkContext context = new JavaSparkContext(conf);

    SparkSession session =
        SparkSession.builder().sparkContext(context.sc()).enableHiveSupport().getOrCreate();

    session.udf().register("myudf", new MyUdf("yurun"), DataTypes.StringType);

    List<String> lines = new ArrayList<>();

    lines.add("a b c");

    JavaRDD<String> source = context.parallelize(lines);

    List<StructField> fields = new ArrayList<>();

    fields.add(DataTypes.createStructField("col1", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("col2", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("col3", DataTypes.StringType, false));

    StructType schema = DataTypes.createStructType(fields);

    JavaRDD<Row> sourceRDD =
        source.map(
            line -> {
              String[] words = line.split(" ", -1);

              return RowFactory.create(words[0], words[1], words[2]);
            });

    Dataset<Row> dataset = session.createDataFrame(sourceRDD, schema);

    dataset.createOrReplaceTempView("source");

    Dataset<Row> results = session.sql("select col1 from source");

    results.createOrReplaceTempView("source");

    results = session.sql("select myudf(col1) from source");

    List<Row> rows = results.collectAsList();

    for (Row row : rows) {
      System.out.println(row);
    }

    session.stop();
  }
}
