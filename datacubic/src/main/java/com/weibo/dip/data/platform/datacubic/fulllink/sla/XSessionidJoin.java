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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Created by yurun on 17/7/25.
 */
public class XSessionidJoin {

    private static final Logger LOGGER = LoggerFactory.getLogger(XSessionidJoin.class);

    private static final String X_SESSION_ID = "X-Sessionid";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> interfaceRDD = context.textFile
            ("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_mwb_interface/2017_07_24/*");

        JavaRDD<String> interfaceXSessionIdRDD = interfaceRDD.flatMap(line -> {
            List<String> xsessionids = new ArrayList<>();

            try {
                String[] words = line.split("`");
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

                                xsessionids.add(xsessionid);
                            }

                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.debug("inteface parse xsessionid error: " + ExceptionUtils.getFullStackTrace(e));
            }

            return xsessionids.iterator();
        });

        JavaRDD<String> performanceLineRDD = context.textFile
            ("/user/hdfs/rawlog/app_weibomobile03x4ts1kl_clientperformance/2017_07_24/*");

        JavaRDD<String> performanceXSessionIdRDD = performanceLineRDD.map(line -> {
            try {
                JsonParser parser = new JsonParser();

                JsonElement lineElement = parser.parse(line);
                if (lineElement.isJsonObject()) {
                    JsonObject jsonObject = lineElement.getAsJsonObject();
                    if (jsonObject.has(X_SESSION_ID)) {
                        return jsonObject.get(X_SESSION_ID).getAsString();
                    }
                }
            } catch (Exception e) {
                LOGGER.debug("performance parse xsessionid error: " + ExceptionUtils.getFullStackTrace(e));
            }

            return null;
        }).filter(Objects::nonNull);

        List<String> xsessionids = interfaceXSessionIdRDD
            .intersection(performanceXSessionIdRDD)
            .top(10000);

        context.stop();

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream
            ("/tmp/xsessionids")));

        for (String xsessionid : xsessionids) {
            writer.write(xsessionid);
            writer.newLine();
        }

        writer.close();
    }

}
