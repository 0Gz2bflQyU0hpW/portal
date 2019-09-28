package com.weibo.dip.data.platform.datacubic.apmloganalysis;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.apmloganalysis.util.DBUtil;
import com.weibo.dip.data.platform.datacubic.apmloganalysis.util.KafkaProducerUtil;
import com.weibo.dip.data.platform.datacubic.apmloganalysis.util.MD5Util;
import com.weibo.dip.data.platform.datacubic.streaming.udf.fulllink.ParseUAInfo;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class ApmFlickLog implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApmFlickLog.class);

    //读日志
    private static String getZookeeperServers() {
        return "10.41.15.21:2181,10.41.15.22:2181,10.41.15.23:2181";
    }
    private static String getConsumerGroup(){ return "apm_flick"; }
    private static String getInPutTopic() { return "mweibo_client_weibo_apm_log"; }

    //卡顿不带contents的日志
    private static String getProcessedBrokers() { return "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092"; }
    private static String getProcessedTopic() { return "dip-kafka2es-common"; }
    //private static String getProcessedBrokers() { return "10.13.4.44:9092"; }
    //private static String getProcessedTopic() { return "page_visits"; }

    //写原始日志
    private static String getOriginBrokers() {
        return "10.41.15.21:9092,10.41.15.22:9092,10.41.15.23:9092,10.41.15.24:9092,10.41.15.25:9092,10.41.15.26:9092,10.41.15.27:9092,10.41.15.28:9092,10.41.15.29:9092,10.41.15.30:9092";
    }
    private static String getOriginTopic() {
        return "mweibo_client_weibo_apm_log_toes";
    }

    private static Map<String, Integer> getSourceTopicMap(){
        Map<String, Integer> map = new HashMap();
        map.put(getInPutTopic(), 4);
        return map;
    }

    private static final String MAPI_URL = "http://crash.intra.weibo.cn/api/crashservice/query?source=crash&env=pro";

    private static String iosFlickTabName = "ios_flick_category";
    private static String andrFlickTabName = "android_flick_category";
    //private static String iosFlickTabName = "ios_flick_test";
    //private static String andrFlickTabName = "android_flick_test";

    public static void main(String[] args) throws Exception{

        ApmFlickLog apm = new ApmFlickLog();

        SparkConf conf = new SparkConf();

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(60000));

        List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            kafkaStreams.add(KafkaUtils.createStream(streamingContext, getZookeeperServers(), getConsumerGroup(),
                    getSourceTopicMap(), StorageLevel.MEMORY_AND_DISK_SER_2()));
        }
        JavaPairDStream<String, String> messages = streamingContext.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));

        //JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(streamingContext, getZookeeperServers(), getConsumerGroup(), getSourceTopicMap());

        JavaDStream<String> msgDStream = messages.map(msg -> msg._2());

        JavaDStream<Map<String, Object>> mapDStream = apm.parseRow(msgDStream);

        //JavaDStream<Map<String, Object>> reDStream = mapDStream.repartition(200);

        JavaDStream<Map<String, Object>> matchDS = apm.computeAndMatch(mapDStream);

        matchDS.persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        //原始日志
        apm.send2Kafka(matchDS);

        JavaDStream<String> updateCategory = apm.getUpdateMsg(matchDS);

        updateNewCategory(updateCategory);

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private JavaDStream<Map<String, Object>> computeAndMatch(JavaDStream<Map<String, Object>> mapDStream){

        JavaDStream<Map<String, Object>> obj = mapDStream.map(map -> {

            if("android".equals(map.get("system"))){
                getAndroidFingerprint(map);
                matchFingerPrint(map, andrFlickTabName);
            }

            if(!"android".equals(map.get("system"))){

                List<ParsedLog> parsedLogs = null;
                try{
                    parsedLogs = transformByMapi(map);
                    getIosFingerPrint(parsedLogs, map);
                    List<ParsedLog> contentBrief = getContentBrief(parsedLogs);
                    map.put("content_brief", StringUtils.join(getContentBrief(contentBrief), '\n'));
                } catch (Exception e){
                    String content = (String)map.get("content_text");
                    parsedLogs = transformContentNotParsed(content);
                    map.put("content_brief", StringUtils.join(parsedLogs, '\n'));
                    map.put("parse_tag", "error");
                }

                matchFingerPrint(map, iosFlickTabName);

                map.put("parse_tag", "success");
            }
            return map;
        });

        return obj.filter(Objects::nonNull);
    }


    private List<ParsedLog> getContentBrief(List<ParsedLog> parsedLogs){

        List<ParsedLog> logs = new ArrayList<>();
        int sum = 0;
        for(ParsedLog line : parsedLogs){
            if(sum >= 5){
                break;
            }
            if(line.isWeibo){
                String[] token = line.getTokens();
                String[] briefToken = null;
                if(token.length >= 4){
                    briefToken = new String[]{token[0], token[2], token[3]};
                } else {
                    briefToken = token;
                }
                ParsedLog parsedLog = new ParsedLog(true, briefToken, line.getDescription(), line.getInfo());
                logs.add(parsedLog);
                sum++;
            }
        }

        if(logs.size() > 0){
            return logs;
        } else {
            return parsedLogs.size() >3 ? parsedLogs.subList(0, 3) : parsedLogs;
        }
    }

    private static void getAndroidFingerprint(Map<String, Object> map){

        String content = (String)map.get("content_text");
        map.put("contents", content);

        String[] contents = content.split("\n");

        List<String> weiboStacks = new ArrayList<>();
        int sum = 0;

        for(String stack : contents){
            if(stack != null && stack.startsWith("com.sina.weibo")){
                weiboStacks.add(stack);
                sum++;
            }
            if(sum >= 5){
                break;
            }
        }

        List<String> fingerPrints = new ArrayList<>();

        int count = weiboStacks.size();

        if(count >0 && count <= 3){
            String descriptions = "";
            for(int index=0; index<count; index++){
                descriptions += weiboStacks.get(index);
            }
            String fp = MD5Util.MD5(descriptions);
            fingerPrints.add(fp);
        }
        if(count > 3){
            for(int index=0; index+2<count; index++){
                if(index > 2){
                    break;
                }

                String fp = MD5Util.MD5(weiboStacks.get(index) + weiboStacks.get(index+1) + weiboStacks.get(index+2));
                fingerPrints.add(fp);
            }
        }

        if(fingerPrints.size() > 0){
            map.put("flick_cause", "weibo");

            Set<String> stackSet = new HashSet<>();
            stackSet.addAll(weiboStacks);
            Set<String> fingerprintSet = new HashSet<>();
            fingerprintSet.addAll(fingerPrints);

            map.put("fp_stack", ("0" + "#" + andrFlickTabName + "#" + StringUtils.join(fingerprintSet, ',') + "#" + StringUtils.join(stackSet, ',')));
        } else {
            map.put("flick_cause", "nonweibo");
        }

        map.put("fingerPrint", StringUtils.join(fingerPrints, ','));

    }


    private List<String> getIosFingerPrint(List<ParsedLog> parsedLog, Map<String, Object> map){

        List<String> description = new ArrayList<>();
        int sum = 0;
        for(ParsedLog line : parsedLog){
            if(sum >= 5){
                break;
            }
            if(line.isWeibo){
                String[] token = line.getTokens();
                description.add(token[0] + " " + token[2] + " " + token[3]);
                sum++;
            }
        }

        List<String> fingerPrints = new ArrayList<>();
        int count = description.size();

        if(count >0 && count < 3){
            String descriptions = "";
            for(int index=0; index<count; index++){
                descriptions += description.get(index);
            }
            String fp = MD5Util.MD5(descriptions);
            fingerPrints.add(fp);
        }
        if(count >= 3){
            for(int index=0; index+2<count; index++){
                if(index > 2){
                    break;
                }
                String fp = MD5Util.MD5(description.get(index) + description.get(index+1) + description.get(index+2));
                fingerPrints.add(fp);
            }
        }

        if(fingerPrints.size() > 0){
            map.put("flick_cause", "weibo");

            Set<String> stackSet = new HashSet<>();
            stackSet.addAll(description);
            Set<String> fingerprintSet = new HashSet<>();
            fingerprintSet.addAll(fingerPrints);

            map.put("fp_stack", ("0" + "#" + iosFlickTabName + "#" + StringUtils.join(fingerprintSet, ',') + "#" + StringUtils.join(stackSet, ',')));
        } else {
            map.put("flick_cause", "nonweibo");
        }

        map.put("fingerPrint", StringUtils.join(fingerPrints, ','));

        return fingerPrints;
    }

    private static void matchFingerPrint(Map<String, Object> resultMap, String tabName){

        String flickCategory = String.valueOf(resultMap.get("fp_stack"));

        if("null".equals(flickCategory)){
            return;
        }

        String[] newCategory = flickCategory.split("#");
        String[] fps = newCategory[2].split(",");

        DBUtil util = new DBUtil();
        Map<String, String> dbMap = util.getCategoryMap(tabName);
        //Map<String, String> dbMap = util.select(tabName);

        boolean flag = false;

        for(String fp : fps){
            if(dbMap != null && dbMap.containsKey(fp)){
                String[] arr = dbMap.get(fp).split("#");
                resultMap.put("flick_category", arr[1]);
                resultMap.put("flick_stacks", StringUtils.join(Arrays.asList(arr[3].split(",")), '\n'));
                flag = true;

                resultMap.remove("fp_stack");
                if("false".equals(arr[4])){
                    String tmparr = computeUpdateParam(dbMap, newCategory, arr, tabName);
                    if(tmparr != null){
                        resultMap.put("fp_stack", tmparr);
                    }
                }
                break;
            }
        }

        if(!flag){
            resultMap.put("flick_category", "nonMatch");
        }
    }

    private static String computeUpdateParam(Map<String, String> dbMap, String[] newCategory, String[] arr, String tabName){

        String fingerprint = arr[2];
        String flick_stacks = arr[3];

        Set<String> stacksSet = new LinkedHashSet<>();
        stacksSet.addAll(Arrays.asList(flick_stacks.split(",")));

        String[] fps = newCategory[2].split(",");
        String[] stacks = newCategory[3].split(",");

        boolean flag = false;

        int len = fingerprint.split(",").length;
        for(String fp : fps){
            if(len<5 && !dbMap.containsKey(fp)){
                fingerprint = fingerprint + "," + fp;
                len++;
                if(!flag){
                    stacksSet.addAll(Arrays.asList(stacks));
                    flag = true;
                }
            }
        }

        String isFull = len==5 ? "true" : "false";

        String res = "1" + "#" + tabName + "#" + fingerprint + "#" + StringUtils.join(stacksSet.toArray(), ",") + "#" + isFull + "#" + arr[0];

        if(flag){
            return res;
        }

        return null;
    }

    private void send2Kafka(JavaDStream<Map<String, Object>> mapDStream){

        mapDStream.foreachRDD(rdd -> rdd.foreachPartition(iterator -> {

            Producer<String, String> producer_origin = KafkaProducerUtil.getInstance(getOriginBrokers());
            Producer<String, String> producer = KafkaProducerUtil.getInstance(getProcessedBrokers());

            while(iterator.hasNext()){
                Map<String, Object> values = iterator.next();

                if(values.containsKey("fp_stack")){
                    values.remove("fp_stack");
                }

                values.put("business", "fulllink-original-summary");
                values.put("timestamp", System.currentTimeMillis());
                values.remove("content_text");

                producer_origin.send(new ProducerRecord<>(getOriginTopic(), GsonUtil.toJson(values)));

                values.put("business", "fulllink-flick-summary");
                producer.send(new ProducerRecord<>(getProcessedTopic(), GsonUtil.toJson(values)));
            }
        }));
    }

    private JavaDStream<String> getUpdateMsg(JavaDStream<Map<String, Object>> mapDStream){

        JavaDStream<String> updateCateGory = mapDStream.flatMap(map -> {
            String str = null;
            if (null != map.get("fp_stack")) {
                str = String.valueOf(map.get("fp_stack"));
            }
            if (str == null) return Collections.emptyIterator();
            else return Arrays.asList(new String[]{str}).iterator();
        });
        return updateCateGory;
    }

    private static void updateNewCategory(JavaDStream<String> category){
        category.repartition(1).foreachRDD(rdd -> {
            rdd.foreachPartition(iter -> {

                DBUtil util = new DBUtil();

                List<String[]> updateList = new ArrayList<>();
                List<String[]> insertList = new ArrayList<>();

                while(iter.hasNext()){

                    String[] arr = iter.next().split("#");

                    String type = arr[0];

                    if("1".equals(type)){
                        updateList.add(arr);
                    } else if ("0".equals(type)){
                        insertList.add(arr);
                    }
                }

                Set<String> set = new HashSet<>();
                Set<String> iosSet = util.getNewestFingerprint(iosFlickTabName);
                Set<String> androidSet = util.getNewestFingerprint(andrFlickTabName);
                set.addAll(iosSet);
                set.addAll(androidSet);

                List<String[]> iosUpdate = new ArrayList<>();
                List<String[]> androidUpdate = new ArrayList<>();
                List<String[]> iosAdd = new ArrayList<>();
                List<String[]> androidAdd = new ArrayList<>();

                for(String[] arr : updateList){
                    String tabName = arr[1];
                    List<String> fps = Arrays.asList(arr[2].split(","));

                    boolean flag =false;
                    for(String fp : fps){
                        if(set.contains(fp)){
                            flag = true;
                            break;
                        }
                    }

                    if(!flag){
                        set.addAll(fps);
                        if(andrFlickTabName.equals(tabName)){
                            androidUpdate.add(arr);
                        } else {
                            iosUpdate.add(arr);
                        }
                    }
                }

                for(String[] arr : insertList){
                    String tabName = arr[1];
                    List<String> fps = Arrays.asList(arr[2].split(","));

                    boolean flag = false;
                    for(String fp : fps){
                        if(set.contains(fp)){
                            flag = true;
                            break;
                        }
                    }

                    if(!flag){
                        set.addAll(fps);
                        if(andrFlickTabName.equals(tabName)){
                            androidAdd.add(arr);
                        } else {
                            iosAdd.add(arr);
                        }
                    }
                }


                util.update(androidUpdate, andrFlickTabName);
                util.update(iosUpdate, iosFlickTabName);

                util.insert(androidAdd, andrFlickTabName);
                util.insert(iosAdd, iosFlickTabName);
            });
        });

    }

    private List<ParsedLog> transformContentNotParsed(String content){

        List<ParsedLog> parseLog = new ArrayList<ParsedLog>();

        if(null != content){

            String[] lines = content.split("#", -1);

            for (String line : lines) {

                String[] tokens = line.split("\\s+", -1);

                parseLog.add(new ParsedLog(false, tokens, null, null));
            }
        }

        return parseLog;
    }

    //所有空记录在这里过滤掉
    private JavaDStream<Map<String, Object>> parseRow(JavaDStream<String> msgDStream){

        JavaDStream<Map<String, Object>> obj = msgDStream.map(row -> {

            Map<String, Object> rowmap = null;
            try{
                rowmap = GsonUtil.fromJson(row, GsonUtil.GsonType.OBJECT_MAP_TYPE);
            } catch (Exception e){
                LOGGER.error("log parse error : ", row);
                return null;
            }

            Map<String, String> ua =  null;
            ParseUAInfo parseUAInfo = new ParseUAInfo();
            try{
                ua =  parseUAInfo.call((String)rowmap.get("ua"));
            } catch (Exception e){
                LOGGER.error("line " + row + " to json error: " + ExceptionUtils.getFullStackTrace(e));
                return null;
            }

            if(ua == null){
                return null;
            }

            String uid = String.valueOf(rowmap.get("uid"));
            if(uid.length() < 9){
                LOGGER.error("uid length error : ", row);
                return null;
            }

            String type = String.valueOf(rowmap.get("type")).toLowerCase();

            if(!"flick".equals(type)){
                return null;
            }

            String from_ = (String)rowmap.get("from");
            String system = from_.substring(from_.length() - 5, from_.length());

            Map<String, Object> line = new HashMap<String, Object>();

            line.put("uid", uid);
            line.put("partial_uid", uid.substring(uid.length()-9, uid.length()-7));
            line.put("type_", type);

            if("android".equals(ua.get("system"))){
                line.put("model", ua.get("mobile_model"));
            } else if(!"NULL".equals(ua.get("mobile_model"))){
                line.put("model", ua.get("mobile_manufacturer") + "," + ua.get("mobile_model"));
            } else {
                line.put("model", ua.get("mobile_manufacturer"));
            }
            line.put("system_version",  ua.get("system_version"));
            line.put("from_", from_);
            line.put("net_type", null != rowmap.get("networktype") ? rowmap.get("networktype") : "NULL");
            line.put("uicode", null != rowmap.get("uicode") ? rowmap.get("uicode") : "NULL");

            if("95010".equals(system)){
                line.put("system", "android");
            } else if("93010".equals(system)){
                line.put("system", "iphone");
            } else {
                line.put("system", "NULL");
            }

            Object tsServerObj = rowmap.get("@timestamp");
            if(tsServerObj != null){
                line.put("@timestamp", String.valueOf(tsServerObj));
            }

            line.put("client_log_timestamp", rowmap.get("client_log_timestamp"));

            Object durObj = rowmap.get("duration");
            if(durObj != null){

                Long duration = 0l;
                String durStr = String.valueOf(durObj);
                try{
                    duration = Long.valueOf(durStr);
                } catch (NumberFormatException e){
                    int index = durStr.indexOf(".");
                    if(index > -1){
                        durStr = durStr.substring(0, index);
                        duration = Long.valueOf(durStr);
                    }
                }

                line.put("duration", duration);

                if(duration > 0 && duration<=199){
                    line.put("duration_range", "0~199");
                } else if(duration>199 && duration<=499){
                    line.put("duration_range", "199~499");
                } else if(duration>499 && duration<=999){
                    line.put("duration_range", "499~999");
                } else {
                    line.put("duration_range", "illegal");
                }
            }

            line.put("total_num", 1);

            if("95010".equals(system)){ //android卡顿
                line.put("content_text",rowmap.get("content"));
            }

            if(!"95010".equals(system)){//ios卡顿
                line.put("content_text", rowmap.get("content"));

                Map<String, Object> sytemmap = null;
                Object sytemJson = rowmap.get("sytem");
                if(null != sytemJson){
                    sytemmap = (Map)sytemJson;

                    line.put("sytem_loadaddress", sytemmap.get("loadaddress"));
                    line.put("sytem_bundleversion", sytemmap.get("bundleversion"));
                    line.put("sytem_arch", sytemmap.get("arch"));
                }
            }

            return line;
        });
        return obj.filter(Objects::nonNull);
    }

    private List<ParsedLog> transformByMapi(Map<String, Object> map) throws Exception {

        List<ParsedLog> parsedlogs = new ArrayList<>();

        String arch = (String)map.get("sytem_arch");
        if(null == arch){
            map.put("content_brief", "NULL");
            return parsedlogs;
        }

        String archInStr = arch.endsWith("64") ? "64" : "32";
        String bundleversion = (String)map.get("sytem_bundleversion");
        String from = (String)map.get("from_");
        String loadaddressInStr = (String)map.get("sytem_loadaddress");
        String content = (String)map.get("content_text");

        if (content != null) {

            String[] lines = content.split("#", -1);
            for (String line : lines) {

                String[] tokens = line.split("\\s+", -1);
                if (tokens.length >= 3 && (tokens[1].startsWith("Weibo") || tokens[1].startsWith("WB"))) {
                    Map<String, String> requestMap = new HashMap<String, String>();
                    long add = Long.decode(tokens[2]);
                    long loadaddress = 0l;
                    if (tokens[1].startsWith("Weibo")) {
                        loadaddress = Long.decode(loadaddressInStr);
                        requestMap.put("arch", archInStr);
                        requestMap.put("addr", "0x" + Long.toHexString(add - loadaddress));
                    } else if (tokens[1].startsWith("WB")) {
                        for (int i = 0; i < tokens.length; i++) {
                            if ("+".equals(tokens[i]) && i < tokens.length - 1) {
                                loadaddress = Long.decode(tokens[i + 1]);
                                break;
                            }
                        }
                        requestMap.put("arch", tokens[1] + "-" + archInStr);
                        requestMap.put("addr", "0x" + Long.toHexString(loadaddress));
                    }

                    if(StringUtils.isNotEmpty(bundleversion) && Integer.valueOf(bundleversion) > 11252){
                        requestMap.put("from", from + "_" + bundleversion);
                    } else {
                        requestMap.put("from", from);
                    }

                    String reponse = doPost(MAPI_URL, requestMap);

                    LOGGER.info("request: {}, response: {}", requestMap, reponse);

                    Map<String, String> result = GsonUtil.fromJson(reponse, GsonUtil.GsonType.STRING_MAP_TYPE);

                    String[] newLine = new String[]{tokens[1], tokens[2], result.get("description"), result.get("info")};
                    ParsedLog parsedLog = new ParsedLog(true, newLine, result.get("description"), result.get("info"));
                    parsedlogs.add(parsedLog);
                } else {
                    ParsedLog parsedLog = new ParsedLog(false, tokens, null, null);
                    parsedlogs.add(parsedLog);
                }
            }
        }
        map.put("contents", StringUtils.join(parsedlogs, '\n'));

        return parsedlogs;
    }


    class ParsedLog {
        boolean isWeibo;
        String[] tokens;
        String description;
        String info;

        public boolean isWeibo() {
            return isWeibo;
        }

        public String[] getTokens() {
            return tokens;
        }

        public String getDescription() {
            return description;
        }

        public String getInfo() {
            return info;
        }

        public void setWeibo(boolean isWeibo) {
            this.isWeibo = isWeibo;
        }

        public void setTokens(String[] tokens) {
            this.tokens = tokens;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void setInfo(String info) {
            this.info = info;
        }

        public ParsedLog(boolean isWeibo, String[] tokens, String description, String info) {
            super();
            this.isWeibo = isWeibo;
            this.tokens = tokens;
            this.description = description;
            this.info = info;
        }

        public String getlog() {
            return StringUtils.join(tokens, ' ');
        }

        @Override
        public String toString() {
            return getlog();
        }

    }

    public String doPost(String url, Map<String, String> params) throws HttpException, IOException {

        if (StringUtils.isEmpty(url)) {
            return null;
        }

        HttpClient client = new HttpClient();
        client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);// MS 3s
        client.getHttpConnectionManager().getParams().setSoTimeout(10 * 60 * 1000);// MS 10min
        client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, CharEncoding.UTF_8);
        client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
        client.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);

        PostMethod method = new PostMethod(url);

        String response = null;

        if (MapUtils.isNotEmpty(params)) {
            List<NameValuePair> pairs = new ArrayList<NameValuePair>();

            for (Map.Entry<String, String> param : params.entrySet()) {
                NameValuePair pair = new NameValuePair(param.getKey(), param.getValue());

                pairs.add(pair);
            }
            method.setRequestBody(pairs.toArray(new NameValuePair[0]));
        }

        try {
            client.executeMethod(method);
            response = read(method.getResponseBodyAsStream());
        } finally {
            method.releaseConnection();
        }

        return response;

    }

    private String read(InputStream in) throws IOException {
        if (in == null) {
            return null;
        }

        List<String> lines = new ArrayList<String>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line = null;

        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }

        StringBuffer sb = new StringBuffer();

        Iterator<String> iterator = lines.iterator();

        while (iterator.hasNext()) {
            sb.append(iterator.next());

            if (iterator.hasNext()) {
                sb.append("\n");
            }
        }

        return sb.toString();
    }

}