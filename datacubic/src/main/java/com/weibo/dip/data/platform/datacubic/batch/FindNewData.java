package com.weibo.dip.data.platform.datacubic.batch;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import io.netty.util.internal.StringUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by delia on 2017/3/24.
 */
public class FindNewData {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindNewData.class);

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> distFile = sparkContext.textFile("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance/2017_03_24/*");
        LongAccumulator sourceCount = sparkContext.sc().longAccumulator();
        LongAccumulator matchCount = sparkContext.sc().longAccumulator();
        LongAccumulator resCount = sparkContext.sc().longAccumulator();

        JavaRDD<String> rdd = distFile.map(line->{

            sourceCount.add(1L);

            Map<String, Object> pairs = null;
            try {
                pairs = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
            } catch (Exception e) {
                LOGGER.debug("Parse line " + line + " with json error: " + ExceptionUtils.getFullStackTrace(e));
            }

            if (MapUtils.isNotEmpty(pairs)) {
                matchCount.add(1L);
                Object value = pairs.get("ne");//18
                Object value1 = pairs.get("lw");//0
                Object value2 = pairs.get("dl");//18
                Object value4 = pairs.get("sc");//18
                Object value5 = pairs.get("ssc");//18
                Object value6 = pairs.get("sch");//18
                Object value7 = pairs.get("sr");//18
                Object value8 = pairs.get("ws");//18
                Object value9 = pairs.get("rh");//18
                Object value10 = pairs.get("rb");//18

//                if ((value != null) && (value1 != null) && (value2 != null) && (value4 != null) && (value5 != null) && (value6 != null) && (value7 != null) && (value8 != null) && (value9 != null) && (value10 != null)){
//                    resCount.add(1L);
//                    return line;
//                }
                if ((value != null) || (value1 != null) || (value2 != null) || (value4 != null) || (value5 != null) || (value6 != null) || (value7 != null) || (value8 != null) || (value9 != null) || (value10 != null)){
                    resCount.add(1L);
                    return line;
                }

            }
            return null;
        }).filter(line-> StringUtils.isNotEmpty(line));

        rdd.repartition(1).saveAsTextFile("/tmp/newdatares/");
        LOGGER.info("all data count: " + sourceCount.value()+ ",match data count: " + matchCount.value()+",res data count: " + resCount.value());

        sparkContext.stop();

    }

}
