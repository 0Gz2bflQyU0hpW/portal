package com.weibo.dip.ml.godeyes;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.MD5Util;
import com.weibo.dip.data.platform.kafka.KafkaReader;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import com.weibo.dip.data.platform.redis.RedisClient;
import ml.dmlc.xgboost4j.java.DMatrix;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yurun on 17/7/12.
 */
public class Handler implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Handler.class);

    private static final String UNDERLINE = "_";
    private static final String COLON = ":";
    private static final String BLANKSPACE = " ";

    private KafkaReader collectReader;
    private RedisClient cacheClient;
    private KafkaWriter monitorWriter;
    private KafkaWriter historyWriter;

    private Predictor predictor;

    private static final String INDEX = "index";
    private static final String INDEX_PREFIX = "dip-godeyes-";

    private static final String TYPE = "type";

    private static final String TIMESTAMP = "timestamp";

    private static final String SERVICE = "service";
    private static final String CVALUE = "cvalue";
    private static final String PVALUE = "pvalue";

    private SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMMdd");
    private SimpleDateFormat utcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    {
        utcDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public Handler(KafkaReader collectReader, RedisClient cacheClient, KafkaWriter monitorWriter,
                   KafkaWriter historyWriter,
                   Predictor predictor) {
        this.collectReader = collectReader;
        this.cacheClient = cacheClient;
        this.monitorWriter = monitorWriter;
        this.historyWriter = historyWriter;

        this.predictor = predictor;
    }

    private boolean check(Record record) {
        return Objects.nonNull(record.getType()) && Objects.nonNull(record.getService());
    }

    private void normalize(Record record) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(record.getTimestamp());

        time.set(Calendar.SECOND, 0);
        time.set(Calendar.MILLISECOND, 0);

        record.setTimestamp(time.getTimeInMillis());
    }

    private int bytesToInt(byte[] bytes) {
        int value = 0;

        value += ((int) bytes[0]) << 24;
        value += ((int) bytes[1]) << 16;
        value += ((int) bytes[2]) << 8;
        value += (int) bytes[3];

        return value;
    }

    private Float[] getService(String service) {
        byte[] bytes = MD5Util.getBytes(service);
        if (ArrayUtils.isEmpty(bytes)) {
            throw new RuntimeException("service " + service + " md5 is null(empty)");
        }

        Float[] values = new Float[4];

        values[0] = (float) bytesToInt(Arrays.copyOfRange(bytes, 0, 4));
        values[1] = (float) bytesToInt(Arrays.copyOfRange(bytes, 4, 8));
        values[2] = (float) bytesToInt(Arrays.copyOfRange(bytes, 8, 12));
        values[3] = (float) bytesToInt(Arrays.copyOfRange(bytes, 12, 16));

        return values;
    }

    private float getWeek(long timestamp) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(timestamp);

        return (float) time.get(Calendar.DAY_OF_WEEK);
    }

    private float getHour(long timestamp) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(timestamp);

        return (float) time.get(Calendar.HOUR_OF_DAY);
    }

    private float getMinute(long timestamp) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(timestamp);

        return (float) time.get(Calendar.MINUTE);
    }

    private Float getValueOfLastDay(String service, long timestamp, int days) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(timestamp);

        time.add(Calendar.DAY_OF_YEAR, -days);

        String value = cacheClient.get(service + UNDERLINE + time.getTimeInMillis());

        if (Objects.nonNull(value)) {
            return Float.valueOf(value);
        } else {
            return null;
        }
    }

    private Float getValueOfLastMinute(String service, long timestamp, int minutes) {
        Calendar time = Calendar.getInstance();

        time.setTimeInMillis(timestamp);

        time.add(Calendar.MINUTE, -minutes);

        String value = cacheClient.get(service + UNDERLINE + time.getTimeInMillis());

        if (Objects.nonNull(value)) {
            return Float.valueOf(value);
        } else {
            return null;
        }
    }

    private List<Float> getFeature(Record record) {
        // service: type + '_' + service
        String service = record.getType() + UNDERLINE + record.getService();
        long timestamp = record.getTimestamp();
        float value = record.getValue();

        List<Float> features = new LinkedList<>();

        /*
            service: 4 floats
         */
        features.addAll(Arrays.asList(getService(service)));

        /*
            week、hour、minute
         */
        features.add(getWeek(timestamp));
        features.add(getHour(timestamp));
        features.add(getMinute(timestamp));


        /*
            last 7 days
         */
        int days = 7;

        for (int day = 1; day <= days; day++) {
            features.add(getValueOfLastDay(service, timestamp, day));
        }

        /*
            last 30 minutes
         */
        int minutes = 30;

        for (int minute = 1; minute <= minutes; minute++) {
            features.add(getValueOfLastMinute(service, timestamp, minute));
        }

        return features;
    }

    private int getNonNUll(List<Float> features) {
        return features.stream()
            .map(feature -> Objects.nonNull(feature) ? 1 : 0)
            .reduce((a, b) -> a + b)
            .orElse(0);
    }

    private DMatrix getMatrix(List<Float> features) throws Throwable {
        int elements = getNonNUll(features);

        long[] rowHeaders = new long[]{0, elements};
        float[] data = new float[elements];
        int[] colIndex = new int[elements];

        int elementIndex = 0;

        for (int featureIndex = 0; featureIndex < features.size(); featureIndex++) {
            Float featureValue = features.get(featureIndex);
            if (Objects.isNull(featureValue)) {
                continue;
            }

            data[elementIndex] = featureValue;
            colIndex[elementIndex] = featureIndex;

            elementIndex++;
        }

        return new DMatrix(rowHeaders, colIndex, data, DMatrix.SparseType.CSR);
    }

    private void monitorAndAlarm(Record record, Float predictValue) {
        Map<String, Object> values = new HashMap<>();

        values.put(INDEX, INDEX_PREFIX + indexDateFormat.format(new Date(System.currentTimeMillis())));
        values.put(TYPE, record.getType());
        values.put(TIMESTAMP, utcDateFormat.format(new Date(record.getTimestamp())));

        values.put(SERVICE, record.getService());
        values.put(CVALUE, record.getValue());

        long pvalue = Long.MIN_VALUE;
        if (Objects.nonNull(predictValue)) {
            pvalue = predictValue.longValue();
        }

        values.put(PVALUE, pvalue);

        monitorWriter.write(GsonUtil.toJson(values));
    }

    private void saveRecordToRedis(Record record) {
        String key = record.getType() + UNDERLINE + record.getService() + UNDERLINE + record.getTimestamp();
        String value = String.valueOf(record.getValue());
        int seconds = 7 * 24 * 3600 + 3600;

        cacheClient.set(key, value, seconds);
    }

    private void saveFeatureToKafka(Record record, List<Float> features) {
        List<String> columns = new ArrayList<>();

        /*
            value
         */
        columns.add(String.valueOf(record.getValue()));

        /*
            Feature
         */
        for (int index = 0; index < features.size(); index++) {
            Float value = features.get(index);
            if (Objects.isNull(value)) {
                continue;
            }

            columns.add(index + COLON + value);
        }

        historyWriter.write(StringUtils.join(columns, BLANKSPACE));
    }

    private void predict(Record record) throws Throwable {
        if (!check(record)) {
            return;
        }

        normalize(record);

        List<Float> features = getFeature(record);

        DMatrix matrix = getMatrix(features);

        Float predictValue = predictor.predict(matrix);

        monitorAndAlarm(record, predictValue);

        saveRecordToRedis(record);

        saveFeatureToKafka(record, features);
    }

    @Override
    public void run() {
        String data;

        while (Objects.nonNull(data = collectReader.read())) {
            try {
                predict(GsonUtil.fromJson(data, Record.class));
            } catch (Throwable e) {
                LOGGER.warn("predict error: {}", ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

}
