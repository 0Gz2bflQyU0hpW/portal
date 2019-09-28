package com.weibo.dip.data.platform.datacubic.druid;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;

/**
 * Created by yurun on 17/1/22.
 */
public class HttpPostForDruid {

    private static String getRequest() throws Exception {
        return StringUtils.join(IOUtils.readLines(HttpPostForDruid.class.getClassLoader().getResourceAsStream("druid.json"), CharEncoding.UTF_8), "\n");
    }

    public static void main(String[] args) throws Exception {
        HttpClient client = new HttpClient();

        client.getHttpConnectionManager().getParams().setConnectionTimeout(3000);
        client.getHttpConnectionManager().getParams().setSoTimeout(10000);

        PostMethod post = new PostMethod("http://77-109-199-bx-core.jpool.sinaimg.cn:18084/druid/v2/?pretty");

        post.addRequestHeader("Content-Type", "application/json");

        String request = getRequest();

        System.out.println(request);

        post.setRequestEntity(new StringRequestEntity(request, null, CharEncoding.UTF_8));

        client.executeMethod(post);

        String response = StringUtils.join(IOUtils.readLines(post.getResponseBodyAsStream(), CharEncoding.UTF_8), "\n");

        post.releaseConnection();

        System.out.println(response);
    }

}
