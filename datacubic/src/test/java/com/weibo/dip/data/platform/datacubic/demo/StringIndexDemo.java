package com.weibo.dip.data.platform.datacubic.demo;

/**
 * Created by xiaoyu on 2017/3/1.
 */
public class StringIndexDemo {
    public static void main(String[] args) {
//        String str = "HUAWEI-HUAWEI M2-803L";
        String str = "-HUAWEIHUAWEI M2803L";
        int i = str.indexOf("-");
        System.out.println(i);
        System.out.println(str.substring(0,i).equals(""));
        System.out.println(str.substring(i+1));
    }
}
