package com.weibo.dip.data.platform.datacubic.batch.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by xiaoyu on 2017/5/15.
 */
class FindName implements UDF1<String,String>{
    private static Set<String> NAMES = new HashSet<>();
    static {
        for (int i = 0; i < 1000; i++) {
            NAMES.add(String.valueOf(i));
        }
    }
    @Override
    public String call(String s) throws Exception {
        if (NAMES.contains(s)){
            return "true";
        }
        Thread.sleep(100000);
        return "false";
    }
}
public class SparkTest {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext context = new JavaSparkContext(new SparkConf());
        JavaRDD<String> rdd = context.parallelize(Arrays.asList("delia","lili","yammi"));
        JavaRDD<Row> rows = rdd.map(line->{
            return RowFactory.create(line);
        });
        List<StructField> strutfields = new ArrayList<>();
        strutfields.add(DataTypes.createStructField("name", DataTypes.StringType,true));
        StructType schema = DataTypes.createStructType(strutfields);
        SparkSession session = SparkSession.builder().sparkContext(context.sc()).enableHiveSupport().getOrCreate();
        session.udf().register("findname",new FindName(), DataTypes.StringType);
        session.createDataFrame(rows,schema).createOrReplaceTempView("name");
        while (true) {
            session.sql("SELECT name,findname(name) FROM name").show(false);
            Thread.sleep(10);
        }


    }
}
