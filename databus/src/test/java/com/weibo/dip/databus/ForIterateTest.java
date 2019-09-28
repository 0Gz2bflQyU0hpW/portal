package com.weibo.dip.databus;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by yurun on 17/8/29.
 */
public class ForIterateTest {

    public static void main(String[] args) {
        Set<String> set = new HashSet<>();

        set.add("key1");
        set.add("key2");
        set.add("key3");

        List<String> values = new ArrayList<>();

        for (String value : set) {
            values.add(value);
        }

        for (String value : values) {
            System.out.println(value);
        }
    }

}
