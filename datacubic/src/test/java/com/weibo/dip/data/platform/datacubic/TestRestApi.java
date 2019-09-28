package com.weibo.dip.data.platform.datacubic;

import com.sina.dip.framework.util.DipSSIGUtil;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import org.apache.commons.lang.CharEncoding;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by xiaoyu on 2017/6/19.
 */


public class TestRestApi {

    private static class QueryResut{
        private int code;
        private String msg;
        private String requestid;
        private String data;

        public QueryResut() {
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

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }

    public static void main(String[] args) throws Exception {
        String domain = "http://api.dip.sina.com.cn";
        String url = "/rest/v2/job/selectjob/getSelectJobResultData/aliyunossCountByIp/aliyunossCountByIp/20170614181441";

        Map<String, String> params = new HashMap<String, String>();

        String accessKey = "oPpAeJ9F4971F73D9B1F";

//        String timestamp = String.valueOf(System.currentTimeMillis()/1000);
        String timestamp = "1497859006";

        String secretkey = "fe36ded203f74c6c8fab5891ef27bd06t9dh4e0u";

        String queryStr = String.format("%s%s?accesskey=%s&timestamp=%s&ssig=%s"
                ,domain
                ,url
                ,accessKey
                ,timestamp
                ,DipSSIGUtil.getSSIG(url, params, accessKey, timestamp, secretkey)
                );

        String result = HttpClientUtil.doGet(queryStr);

        System.out.println("result : " + result);

        if (Objects.isNull(result)) {
            return;
        }

        Map<String, Object> response = GsonUtil.fromJson(result, GsonUtil.GsonType.OBJECT_MAP_TYPE);

        String code = (String) response.getOrDefault("code", null);
        String msg = (String) response.getOrDefault("msg", null);

        if (!code.equals("200")) {
            System.out.println("response: " + code + ", " + msg);

            return;
        }

        QueryResut bean = GsonUtil.fromJson(result, QueryResut.class);

        System.out.println(bean.getData());


    }
}
