package com.weibo.dip.data.platform.datacubic.videotrace.ha;

/**
 * Created by yurun on 17/1/19.
 */
public class HAResultSourceMain {

    private static void writeToKafka() throws Exception {
//        Map<String, Object> config = new HashMap<>();
        //
        //        config.put("bootstrap.servers", "10.210.136.34:9092,10.210.136.80:9092,10.210.77.15:9092");
        //
        //        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //
        //        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //
        //        Producer<String, String> producer = new KafkaProducer<>(config);
        //
        //        String topic = "video-ha";
        //
        //        DruidClient client = new DruidClient("d013004044.hadoop.dip.weibo.com", 18082, 3000, 10000);
        //
        //        DataSource dataSource = new TableDatasource("videotrace_ha_result");
        //
        //        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //
        //        Date startTime = format.parse("2017-02-07 00:00:00");
        //
        //        Date endTime = format.parse("2017-02-07 23:59:59");
        //
        //        Interval interval = new Interval(startTime, endTime);
        //
        //        Select selectQuery = QueryBuilderFactory.createSelectQueryBuilder()
        //            .setDataSource(dataSource)
        //            .addInterval(interval)
        //            .build();
        //
        //        List<DruidClient.Event> events = client.select(selectQuery, DruidClient.Event.class);
        //
        //        for (DruidClient.Event event : events) {
        //            Map<String, String> data = new HashMap<>();
        //
        //            data.put("index", "video-ha-dip");
        //            data.put("type", "video-ha");
        //            data.put("data", GsonUtil.toJson(event));
        //
        //            String line = GsonUtil.toJson(data);
        //
        //            producer.send(new ProducerRecord<>(topic, line));
        //        }
        //
        //        producer.close();
        //
        //        System.out.println("send success: " + events.size());
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            writeToKafka();

            Thread.sleep(10000);
        }
    }

}
