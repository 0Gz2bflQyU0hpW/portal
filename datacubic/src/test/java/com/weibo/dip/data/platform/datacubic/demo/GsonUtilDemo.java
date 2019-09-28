package com.weibo.dip.data.platform.datacubic.demo;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

import java.util.Map;

/**
 * Created by xiaoyu on 2017/3/1.
 */
public class GsonUtilDemo {

    public static void main(String[] args) {
        String str = "{\"result_code\":\"0\",\"__date\":1488314740.7164,\"groupid\":\"100015342746457\",\"netDataCount\":29,\"during_time\":948.44996929169,\"start_time\":510007539.76762,\"subtype\":\"refresh_feed\",\"parseTime\":171.55295610428,\"network_type\":\"4g\",\"net_time\":\"716.7868614196777\",\"ip\":\"112.17.240.184\",\"ua\":\"iPhone9,1__weibo__7.1.0__iphone__os10.0.3\",\"from\":\"1071093010\",\"networktype\":\"4g\",\"uid\":\"5342746457\"}";

        Map<String,Object> paris = GsonUtil.fromJson(str,GsonUtil.GsonType.OBJECT_MAP_TYPE);
        System.out.println(paris.get("__date").getClass().getName());
    }
}
