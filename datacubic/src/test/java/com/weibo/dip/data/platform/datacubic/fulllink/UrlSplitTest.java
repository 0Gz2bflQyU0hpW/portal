package com.weibo.dip.data.platform.datacubic.fulllink;

/**
 * Created by yurun on 17/7/6.
 */
public class UrlSplitTest {

    public static void main(String[] args) {
        String url = "https://api.weibo.cn/2/statuses/unread_friends_timeline?&wm=3333_2001";
        String url2 = "https://api.weibo.cn/2/statuses/unread_friends_timeline";
        String url3 = null;

        System.out.println(url.split("\\?")[0]);
        System.out.println(url2.split("\\?")[0]);
        System.out.println(url3.split("\\?")[0]);
    }

}
