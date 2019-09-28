package com.weibo.dip.ml.godeyes;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.kafka.KafkaWriter;
import com.weibo.dip.ml.commons.TerminationSignal;

import java.util.Random;

/**
 * Created by yurun on 17/6/28.
 */
public class GodEyesInputSimulator {

    private static final Random RANDOM = new Random();

    private static long generateValue() {
        return RANDOM.nextInt(Integer.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        String[] services = {"serivce_1", "service_2", "service_3"};

        boolean stoped = false;

        String servers = "10.13.4.44:9092";
        String topic = "godeyes_collect";

        String type = "default";

        KafkaWriter writer = new KafkaWriter(servers, topic);

        TerminationSignal signal = new TerminationSignal();

        Runtime.getRuntime().addShutdownHook(new Thread(signal::terminate));

        while (!signal.isTerminated()) {
            for (String service : services) {
                Record record = new Record();

                record.setType(type);
                record.setService(service);
                record.setTimestamp(System.currentTimeMillis());
                record.setValue(generateValue());

                String line = GsonUtil.toJson(record, Record.class);

                writer.write(line);
            }

            Thread.sleep(1000);
        }

        writer.close();
    }

}
