package com.weibo.dip.data.platform.datacubic.batch.util;

import com.weibo.dip.data.platform.datacubic.batch.BatchEngineSample;
import com.weibo.dip.data.platform.datacubic.batch.batchpojo.BatchConstant;
import com.weibo.dip.data.platform.datacubic.batch.Destination;
import com.weibo.dip.data.platform.datacubic.streaming.mapper.RowMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by xiaoyu on 2017/3/13.
 */
public class FunctionsUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(FunctionsUtil.class);

    public static StructType createSchema(String[] COLUMNS) {
        if (ArrayUtils.isEmpty(COLUMNS)) {
            return null;
        }

        List<StructField> fields = new ArrayList<>();

        for (String columnName : COLUMNS) {
            fields.add(DataTypes.createStructField(columnName, DataTypes.StringType, true));
        }

        return DataTypes.createStructType(fields);
    }

    public static Map<String, RowMapper> getRowMappers() throws Exception {
        Properties properties = new Properties();

        properties.load(new BufferedReader(new InputStreamReader(BatchEngineSample.class.getClassLoader().getResourceAsStream(BatchConstant.MAPPERS_CONFIG), CharEncoding.UTF_8)));

        Map<String, RowMapper> mappers = new HashMap<>();

        for (String name : properties.stringPropertyNames()) {
            String classImpl = properties.getProperty(name);

            mappers.put(name, (RowMapper) Class.forName(classImpl).newInstance());
        }

        return mappers;
    }

    public static void writeOut(List<Row> rows, Destination destination) throws Exception {
        int size = CollectionUtils.isEmpty(rows) ? 0 : rows.size();

        LOGGER.info("size: " + size);

        RowMapper mapper = destination.getMapper();

        String outputPathStr = destination.getOutputPath();

        Path outputPath = new Path(outputPathStr);

        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {

            fs.delete(outputPath, true);

            outputPath = new Path(outputPathStr);

            LOGGER.info("outputPath " + outputPath + " already exists, deleted");
        }

        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath), CharEncoding.UTF_8));

            for (Row row : rows) {
                String line = mapper.map(row);
                writer.write(line);
                writer.newLine();
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
