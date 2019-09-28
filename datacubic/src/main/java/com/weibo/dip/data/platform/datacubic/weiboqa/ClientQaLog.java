package com.weibo.dip.data.platform.datacubic.weiboqa;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.apmloganalysis.util.KafkaProducerUtil;
import java.io.Serializable;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class ClientQaLog implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientQaLog.class);

  //private static final String zkstr = "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001";
  private static final String zkstr = "10.41.15.21:2181,10.41.15.22:2181,10.41.15.23:2181";
  private static final String consumerGroup = "dip_qa";
  private static final String topics = "mweibo_client_medialive_qa_log";

  private static Map<String, Integer> getTopic() {
    Map<String, Integer> map = new HashMap<>();
    map.put(topics, 3);
    return map;
  }

  private static String getProcessedBrokers() {
    return "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092";
  }

  private static String getProcessedTopic() {
    return "dip-kafka2es-common";
  }

  public static void main(String[] args) throws Exception {

    ClientQaLog clientQaLog = new ClientQaLog();

    SparkConf conf = new SparkConf();

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));

    JavaPairInputDStream<String, String> messages = KafkaUtils
        .createStream(streamingContext, zkstr, consumerGroup, getTopic());

    JavaDStream<String> lines = messages.map((Function<Tuple2<String, String>, String>) Tuple2::_2);

    JavaDStream<Map<String, Object>> parsedRow = clientQaLog.parseRow(lines);

    clientQaLog.send2Kafka(parsedRow);

    streamingContext.start();
    streamingContext.awaitTermination();
  }

  private JavaDStream<Map<String, Object>> parseRow(JavaDStream<String> msgDStream) {

    JavaDStream<Map<String, Object>> obj = msgDStream.map(row -> {

      Map<String, Object> rowmap;
      try {
        rowmap = GsonUtil.fromJson(row, GsonUtil.GsonType.OBJECT_MAP_TYPE);
      } catch (Exception e) {
        LOGGER.error("log_parse_error : ", row);
        return null;
      }

      Map<String, Object> line = new HashMap<>();

      line.put("@timestamp", rowmap.get("@timestamp"));

      String uid = filterStringType(rowmap.get("medialive_qa_uid"));
      if (uid != null) {
        line.put("medialive_qa_uid", uid);
      }

      String act = filterStringType(rowmap.get("act"));
      if (act != null) {
        line.put("act", act);
      }

      String type = filterStringType(rowmap.get("medialive_qa_type"));
      if (type != null) {
        line.put("medialive_qa_type", type);
      }

      String platform = filterStringType(rowmap.get("medialive_qa_platform"));
      if (platform != null) {
        line.put("medialive_qa_platform", platform.toLowerCase());
      }

      Long sys_msg_type = filterLongType(rowmap.get("medialive_qa_sys_msg_type"));
      if (sys_msg_type != null) {
        line.put("medialive_qa_sys_msg_type", sys_msg_type);
      }

      Long log_type = filterLongType(rowmap.get("medialive_qa_log_type"));
      if (log_type != null) {
        line.put("medialive_qa_log_type", log_type);
      }

      Long status_after = filterLongType(rowmap.get("medialive_qa_status_after"));
      if (status_after != null) {
        line.put("medialive_qa_status_after", status_after);
      }

      Long status_before = filterLongType(rowmap.get("medialive_qa_status_before"));
      if (status_before != null) {
        line.put("medialive_qa_status_before", status_before);
      }

      if(rowmap.containsKey("medialive_qa_answer_show")){
        Object answer_show = rowmap.get("medialive_qa_answer_show");
        if (Double.class.equals(answer_show.getClass())) {
          line.put("medialive_qa_answer_show", transformTimestamp2Date((Double) answer_show));
        } else if (String.class.equals(answer_show.getClass())
            && answer_show.toString().length() == 19) {
          line.put("medialive_qa_answer_show", answer_show);
        }
      }

      if(rowmap.containsKey("medialive_qa_answer_receive")){
        Object answer_receive = rowmap.get("medialive_qa_answer_receive");
        if (Double.class.equals(answer_receive.getClass())) {
          line.put("medialive_qa_answer_receive", transformTimestamp2Date((Double) answer_receive));
        } else if (String.class.equals(answer_receive.getClass())
            && answer_receive.toString().length() == 19) {
          line.put("medialive_qa_answer_receive", answer_receive);
        }
      }

      Long retry = filterLongType(rowmap.get("medialive_qa_retry"));
      if (retry != null) {
        line.put("medialive_qa_retry", retry);
      }

      Long send_result = filterLongType(rowmap.get("medialive_qa_send_result"));
      if (send_result != null) {
        line.put("medialive_qa_send_result", send_result);
      }

      if(rowmap.containsKey("medialive_qa_send_answer_response")){
        Object send_answer_response = rowmap.get("medialive_qa_send_answer_response");
        if (Double.class.equals(send_answer_response.getClass())) {
          line.put("medialive_qa_send_answer_response",
              transformTimestamp2Date((Double) send_answer_response));
        } else if (String.class.equals(send_answer_response.getClass())
            && send_answer_response.toString().length() == 19) {
          line.put("medialive_qa_send_answer_response", send_answer_response);
        }
      }

      if(rowmap.containsKey("medialive_qa_send_answer")){
        Object send_answer = rowmap.get("medialive_qa_send_answer");
        if (Double.class.equals(send_answer.getClass())) {
          line.put("medialive_qa_send_answer", transformTimestamp2Date((Double) send_answer));
        } else if (String.class.equals(send_answer.getClass())
            && send_answer.toString().length() == 19) {
          line.put("medialive_qa_send_answer", send_answer);
        }
      }

      String user_answer = filterStringType(rowmap.get("medialive_qa_user_answer"));
      if (user_answer != null) {
        line.put("medialive_qa_user_answer", user_answer);
      }

      String question_id = filterStringType(rowmap.get("medialive_qa_question_id"));
      if (question_id != null) {
        line.put("medialive_qa_question_id", question_id);
      }

      String question_no = filterStringType(rowmap.get("medialive_qa_question_no"));
      if (question_no != null) {
        line.put("medialive_qa_question_no", question_no);
      }

      if(rowmap.containsKey("medialive_qa_question_show")){
        Object question_show = rowmap.get("medialive_qa_question_show");
        if (Double.class.equals(question_show.getClass())) {
          line.put("medialive_qa_question_show", transformTimestamp2Date((Double) question_show));
        } else if (String.class.equals(question_show.getClass())
            && question_show.toString().length() == 19) {
          line.put("medialive_qa_question_show", question_show);
        }
      }
      if(rowmap.containsKey("medialive_qa_question_receive")){
        Object question_receive = rowmap.get("medialive_qa_question_receive");
        if (Double.class.equals(question_receive.getClass())) {
          line.put("medialive_qa_question_receive",
              transformTimestamp2Date((Double) question_receive));
        } else if (String.class.equals(question_receive.getClass())
            && question_receive.toString().length() == 19) {
          line.put("medialive_qa_question_receive", question_receive);
        }
      }

      String medialive_qa_version = filterStringType(rowmap.get("medialive_qa_version"));
      if (medialive_qa_version != null) {
        line.put("medialive_qa_version", medialive_qa_version);
      }

      if(rowmap.containsKey("medialive_qa_logtime")){
        Object medialive_qa_logtime = rowmap.get("medialive_qa_logtime");
        if (Double.class.equals(medialive_qa_logtime.getClass())) {
          line.put("medialive_qa_logtime",
              transformTimestamp2Date((Double) medialive_qa_logtime));
        }
      }

      String medialive_qa_datas;
      try {
        medialive_qa_datas = (String) rowmap.get("medialive_qa_datas");
      } catch (Exception e) {
        try {
          medialive_qa_datas = GsonUtil.toJson(rowmap.get("medialive_qa_datas"));
        } catch (Exception e1) {
          ExceptionUtils.getFullStackTrace(e1);
          return line;
        }
      }

      if (medialive_qa_datas != null) {
        line.put("medialive_qa_datas", medialive_qa_datas);

        Pattern ptn_dis_suc = Pattern.compile("(dispatch_success.+?)(,)");
        Matcher matcher_dis = ptn_dis_suc.matcher(medialive_qa_datas);
        String dispatch_success = "";
        if (matcher_dis.find()) {
          String[] arr = matcher_dis.group(1).replace(" ", "").split(":");
          dispatch_success = arr.length > 1 ? arr[1] : null;
          if (dispatch_success != null && !"".equals(dispatch_success)) {
            line.put("dispatch_success", dispatch_success);
          }
        }

        Pattern ptn_conn_status = Pattern.compile("(connect_status.+?)(,)");
        Matcher matcher_con = ptn_conn_status.matcher(medialive_qa_datas);
        if (matcher_con.find()) {
          String[] arr = matcher_con.group(1).replace(" ", "").split(":");
          String connect_status = arr.length > 1 ? arr[1] : null;
          if (connect_status != null) {
            line.put("connect_status", connect_status);
          }
        }

        if (platform != null && "ios".equals(platform.toLowerCase()) && "false"
            .equals(dispatch_success)) {
          medialive_qa_datas = medialive_qa_datas.replace("\\n", "");

          Pattern pattern_err_msg = Pattern.compile("(Error Domain=.+?)(},)");
          Matcher match_err_msg = pattern_err_msg.matcher(medialive_qa_datas);
          if (match_err_msg.find()) {
            line.put("error_message", match_err_msg.group(1) + "}");
          }
        }

      }

      return line;
    });
    return obj.filter(Objects::nonNull);
  }

  private String filterStringType(Object obj) {

    if (obj == null) {
      return null;
    }

    if (String.class.equals(obj.getClass())) {
      return obj.toString();
    } else {
      return null;
    }
  }

  private Long filterLongType(Object obj) {

    if (obj == null) {
      return null;
    }

    if (Long.class.equals(obj.getClass())) {
      return (Long) obj;
    } else if (String.class.equals(obj.getClass())) {
      try {
        return Long.valueOf(obj.toString());
      } catch (NumberFormatException e) {
        return null;
      }
    } else {
      return null;
    }
  }

  private String transformTimestamp2Date(Double timestamp){

    NumberFormat numFormat = NumberFormat.getInstance();
    numFormat.setGroupingUsed(false);
    String time = numFormat.format(timestamp).split("\\.")[0] + "000";

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = new Date(Long.valueOf(time));

    return simpleDateFormat.format(date);
  }

  private void send2Kafka(JavaDStream<Map<String, Object>> mapDStream) {

    mapDStream.foreachRDD(rdd -> rdd.foreachPartition(iterator -> {

      @SuppressWarnings("unchecked")
      Producer<String, String> producer = KafkaProducerUtil.getInstance(getProcessedBrokers());

      while (iterator.hasNext()) {
        Map<String, Object> values = iterator.next();
        values.put("business", "dip-client-qa-log");
        values.put("timestamp", System.currentTimeMillis());
        producer.send(new ProducerRecord<>(getProcessedTopic(), GsonUtil.toJson(values)));
      }
    }));
  }

}
