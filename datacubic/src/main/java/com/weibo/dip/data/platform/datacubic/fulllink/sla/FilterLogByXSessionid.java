package com.weibo.dip.data.platform.datacubic.fulllink.sla;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by yurun on 17/7/25.
 */
public class FilterLogByXSessionid {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterLogByXSessionid.class);

    private static final String X_SESSION_ID = "X-Sessionid";

    public static void main(String[] args) throws Exception {
        Set<String> xsessionids = new HashSet<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream("/tmp/xsessionids")));

        String line;

        while ((line = reader.readLine()) != null) {
            xsessionids.add(line);
        }

        reader.close();

        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> interfaceRDD = context.textFile
            ("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_mwb_interface/2017_07_24/*");

        interfaceRDD
            .filter(json -> {
                try {
                    String[] words = json.split("`");
                    if (ArrayUtils.isNotEmpty(words) && words.length == 2) {
                        String jsonArrayStr = words[0];

                        JsonParser parser = new JsonParser();

                        JsonElement lineElement = parser.parse(jsonArrayStr);
                        if (lineElement.isJsonArray()) {
                            JsonArray jsonArray = lineElement.getAsJsonArray();

                            Iterator<JsonElement> iterator = jsonArray.iterator();

                            while (iterator.hasNext()) {
                                JsonObject jsonObject = iterator.next().getAsJsonObject();
                                if (jsonObject.has(X_SESSION_ID)) {
                                    String xsessionid = jsonObject.get(X_SESSION_ID).getAsString();
                                    if (xsessionids.contains(xsessionid)) {
                                        return true;
                                    }
                                }

                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.debug("inteface parse xsessionid error: " + ExceptionUtils.getFullStackTrace(e));
                }

                return false;
            })
            .repartition(1)
            .saveAsTextFile("/tmp/interface");

        JavaRDD<String> performanceLineRDD = context.textFile
            ("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance/2017_07_24/*");

        performanceLineRDD
            .filter(json -> {
                try {
                    JsonParser parser = new JsonParser();

                    JsonElement lineElement = parser.parse(json);
                    if (lineElement.isJsonObject()) {
                        JsonObject jsonObject = lineElement.getAsJsonObject();
                        if (jsonObject.has(X_SESSION_ID)) {
                            String xsessionid = jsonObject.get(X_SESSION_ID).getAsString();
                            if (xsessionids.contains(xsessionid)) {
                                return true;
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.debug("performance parse xsessionid error: " +
                        ExceptionUtils.getFullStackTrace(e));
                }

                return false;
            })
            .repartition(1)
            .saveAsTextFile("/tmp/performance");

        context.stop();
    }

}
