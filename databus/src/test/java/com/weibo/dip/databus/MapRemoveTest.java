package com.weibo.dip.databus;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yurun on 17/8/29.
 */
public class MapRemoveTest {

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();

        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        Iterator<Map.Entry<String, String>> iter = map.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();

            System.out.println(entry.getKey() + " : " + entry.getValue());

            iter.remove();
        }
    }

}
