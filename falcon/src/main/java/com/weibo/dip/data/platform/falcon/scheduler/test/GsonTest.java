package com.weibo.dip.data.platform.falcon.scheduler.test;

import com.google.gson.Gson;

/**
 * Created by Wen on 2017/1/19.
 *
 */
public class GsonTest {
    public static void main(String[] args) {
        Gson gson = new Gson();
        User user = new User();
        String json2Object = gson.toJson(user);
        System.out.println(json2Object);
        User user1 = gson.fromJson(json2Object,User.class);
        System.out.println(user1);
    }

}
