package com.weibo.dip.data.platform.datacubic;

import java.io.*;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xiaoyu on 2017/6/1.
 */
public class LogSplit {

    public static void main(String[] args) throws ParseException, IOException {
        String split = "\\|##\\|";
        String content = "_accesskey=sinaedgeahsolci14ydn&_ip=202.102.94.108&_port=80&_an=LC16090092&_data=us.sinaimg.cn 49.67.216.213 0.064s TCP_MISS:CP_HIT [26/May/2017:23:58:56 +0800] \"GET /000PCHFFjx07aEDMRQ6s010401000BYl0k01.mp4?label=inch_5_mp4_hd&Expires=1495817795&ssig=uoYKtUf6UC&KID=unistore,video HTTP/1.1\" 206 74081 \"-\" \"-\" \"-\" \"MI 4LTE_4.4.4_weibo_7.5.1_android_wifi\" \"202.102.94.54:80\" 0.024 \"80\" [-:-:-:-]";

        Pattern pattern = Pattern.compile("^_accesskey=([^=]*)&_ip=([^=]*)&_port=([^=]*)&_an=([^=]*)&_data=([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) \\[([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) ([^\\s]*) (.*)$");

        Matcher matcher = pattern.matcher(content);
        if (matcher.matches()){
            System.out.println("match");
            for (int i = 1; i <= 18; i++) {
                System.out.println(i + ":  " +matcher.group(i));
            }
        }else{
            System.out.println("not match");
        }
    }

}
