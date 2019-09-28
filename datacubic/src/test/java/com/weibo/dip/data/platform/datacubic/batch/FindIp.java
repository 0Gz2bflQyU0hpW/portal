package com.weibo.dip.data.platform.datacubic.batch;

/**
 * Created by xiaoyu on 2017/5/16.
 */
public class FindIp {

    public static void main(String[] args) {
        String src = "_accesskey=sinaedgeahsolci14ydn&_ip=124.228.42.162&_port=80&_an=YY14070796&_data=travel.sina.com.cn - 0 TCP_HIT [11/May/2017:00:57:24 +0800] \"GET /china/2014-05-20/1622262936.shtml HTTP/0.0\" 200 110912 \"-\" \"-\" \"-\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9.0.3) Gecko/2008092417 Firefox/3.0.3\" *Not IP address [0]*";
//        String src = "&_data=20170511.01h03m32s RESPONSE: sent 221.236.30.91 status 404 (Not Found on Accelerator) for 'http://blog.sina.com.cn/blog7common/js/boot.js'";
        int strt,end,date;
//        System.out.println(strt = src.indexOf("_ip="));
//        System.out.println(end = src.indexOf("&_port="));
//        System.out.println(src.substring(strt + 4,end));d
        System.out.println(date = src.indexOf("11/May/2017"));
        System.out.println(src.substring(date + 6,date + 6 + 8));
    }
}
