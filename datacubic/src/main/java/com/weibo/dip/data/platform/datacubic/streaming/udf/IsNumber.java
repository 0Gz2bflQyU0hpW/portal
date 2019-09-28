package com.weibo.dip.data.platform.datacubic.streaming.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/3/29.
 */
public class IsNumber implements UDF1<String, Boolean> {

    private static final Pattern INTEGER = Pattern.compile("^[-|+]?\\d+$");

    private static final Pattern DECIMAL = Pattern.compile("^[-|+]?\\d+\\.\\d+$");

    @Override
    public Boolean call(String value) throws Exception {
        return Objects.nonNull(value) && (INTEGER.matcher(value).matches() || DECIMAL.matcher(value).matches());
    }

    public static void main(String[] args) throws Exception {
        IsNumber isNumber = new IsNumber();

        System.out.println(isNumber.call("10"));
        System.out.println(isNumber.call("+10"));
        System.out.println(isNumber.call("-10"));

        System.out.println(isNumber.call("10.0"));
        System.out.println(isNumber.call("+10.00"));
        System.out.println(isNumber.call("-10.000"));
    }

}
