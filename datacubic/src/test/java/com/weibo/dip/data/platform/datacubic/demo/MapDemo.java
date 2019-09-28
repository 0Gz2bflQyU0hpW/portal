package com.weibo.dip.data.platform.datacubic.demo;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiaoyu on 2017/2/27.
 */
public class MapDemo {
    public static void main(String[] args) {
        Map<String, String> pairs = new HashMap<>();
        pairs.put("kay","value");
        pairs.put("key","");
        System.out.println(pairs.get("key1"));

    }
}
