package com.weibo.dip.data.platform.datacubic.fulllink;

/**
 * Created by yurun on 17/6/8.
 */
public class ReplaceTimestamp {

    public static void main(String[] args) {
        String str = "value: @TIMESTAMP";

        String result = str.replaceAll("@TIMESTAMP", String.valueOf(System.currentTimeMillis()));

        System.out.println(result);
    }

}
