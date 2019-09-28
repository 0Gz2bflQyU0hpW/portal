package com.weibo.dip.databus.hdfstokafka;

import com.sina.dip.framework.util.DipSSIGUtil;
import com.sina.dip.framework.util.GsonUtil;
import com.sina.dip.framework.util.HttpClientUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yurun on 17/11/1.
 */
public class Main {

    public static List<String> getRecentFiles(String category, long start, long end) throws Exception {
        String host = "api.eos.dip.sina.com.cn";
        String port = "8083";

        String api = "/rest/v2/yunwei/getrangetimefiles";

        String type = "rawlog";

        Map<String, String> params = new HashMap<>();

        params.put("category", category);
        params.put("start", String.valueOf(start));
        params.put("end", String.valueOf(end));
        params.put("type", type);

        String accessKey = "oPpAeJ9F4971F73D9B1F";

        String timestamp = "1509507893";

        String secretkey = "fe36ded203f74c6c8fab5891ef27bd06t9dh4e0u";

        String ssig = DipSSIGUtil.getSSIG(api, params, accessKey, timestamp, secretkey);

        String url = String.format(
            "http://%s:%s%s?category=%s&end=%s&start=%s&type=%s&accesskey=%s&timestamp=%s&ssig=%s"
            , host, port, api, category, end, start, type, accessKey, timestamp, ssig);

        String result = HttpClientUtil.doGet(url);

        Map<String, Object> datas = GsonUtil.fromJson(result, GsonUtil.GsonType.OBJECT_MAP_TYPE);

        System.out.println(datas.get("data").getClass().getName());

        return null;
    }

    public static void main(String[] args) throws Exception {
        getRecentFiles("app_weibomobile03x4ts1kl_clientperformance", 1509507000, 1509507300);
    }

}
