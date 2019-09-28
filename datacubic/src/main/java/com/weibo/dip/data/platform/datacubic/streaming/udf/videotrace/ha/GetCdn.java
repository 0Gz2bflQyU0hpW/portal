package com.weibo.dip.data.platform.datacubic.streaming.udf.videotrace.ha;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/1/19.
 */
public class GetCdn implements UDF1<String, String> {

    private static final Pattern CDN_PATTERN = Pattern.compile("f=([^,;]+)");

    @Override
    public String call(String via) throws Exception {
        List<String> cdns = new ArrayList<>();

        Matcher matcher = CDN_PATTERN.matcher(via);

        while (matcher.find()) {
            cdns.add(matcher.group(1));
        }

        String cdn = String.join("_", cdns);

        return StringUtils.isEmpty(cdn) ? "Other" : cdn;
    }

    public static void main(String[] args) throws Exception {
        String line = "";

        GetCdn getCdn = new GetCdn();

        String result = getCdn.call(line);

        System.out.println(result);
    }

}
