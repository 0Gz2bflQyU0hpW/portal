package com.weibo.dip.data.platform.datacubic.test;

import com.weibo.dip.data.platform.commons.util.GsonUtil;

import java.util.Map;

/**
 * Created by yurun on 17/4/19.
 */
public class GsonUtilTestMain {

    public static void main(String[] args) {
        Map<String, Object> values = GsonUtil.fromJson("", GsonUtil.GsonType.OBJECT_MAP_TYPE);

        System.out.println(values);
    }

}
