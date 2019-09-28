package com.weibo.dip.data.platform.falcon.transport.utils;

import com.weibo.dip.data.platform.commons.util.DipSsigUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import org.apache.commons.lang.CharEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;


/**
 * Created by Wen on 2017/3/7.
 */
public class ProducerTaskUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerTaskUtils.class);

    private static class HdfsURLRequest {

        private int code;
        private String msg;
        private String requestid;
        private List<String> data;

        public HdfsURLRequest() {

        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public String getRequestid() {
            return requestid;
        }

        public void setRequestid(String requestid) {
            this.requestid = requestid;
        }

        public List<String> getData() {
            return data;
        }

        public void setData(List<String> data) {
            this.data = data;
        }

    }

    /**
     * 获取最近完成的文件列表，时间精确到秒级，区间为闭区间
     * @param category 数据集名称
     * @param begin 起始时间
     * @param end 结束时间
     * @return
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public static List<String> getRecentFinishedLogByURL(String category, long begin, long end) throws InvalidKeyException, NoSuchAlgorithmException, IOException {
        String startStr = String.valueOf(begin / 1000);
        String endStr = String.valueOf(end / 1000);

        String method = "/rest/v2/yunwei/getrangetimefiles";

        Map<String, String> params = new HashMap<>();

        params.put("type", "rawlog");
        params.put("category", category);
        params.put("start", startStr);
        params.put("end", endStr);

        String accessKey = "oPpAeJ9F4971F73D9B1F";

        String timestamp = Long.toString(new Date().getTime() / 1000);

        String secretkey = "fe36ded203f74c6c8fab5891ef27bd06t9dh4e0u";

        String url = "http://api.dip.sina.com.cn" + method + "?&type=rawlog&category="
                + category + "&start=" + startStr + "&end=" + endStr + "&accesskey=oPpAeJ9F4971F73D9B1F&timestamp=" +
                timestamp + "&ssig=" + DipSsigUtil.getSsig(method, params, accessKey, timestamp, secretkey);

        String result = HttpClientUtil.doGet(url, CharEncoding.UTF_8);

        LOGGER.debug("result: " + result);

        if (Objects.isNull(result)) {
            return null;
        }

        Map<String, Object> response = GsonUtil.fromJson(result, GsonUtil.GsonType.OBJECT_MAP_TYPE);

        String code = (String) response.getOrDefault("code", null);
        String msg = (String) response.getOrDefault("msg", null);

        if (!code.equals("200")) {
            LOGGER.warn("response: " + code + ", " + msg);

            return null;
        }

        HdfsURLRequest bean = GsonUtil.fromJson(result, HdfsURLRequest.class);

        return bean.getData();
    }

    private ProducerTaskUtils() {

    }
}
