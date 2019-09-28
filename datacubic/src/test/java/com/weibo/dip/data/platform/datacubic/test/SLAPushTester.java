package com.weibo.dip.data.platform.datacubic.test;

import org.apache.commons.lang.CharEncoding;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;

/**
 * Created by yurun on 17/5/10.
 */
public class SLAPushTester {

    public static void main(String[] args) throws Exception {
        String host = "10.39.40.39";
        int port = 2503;

        String pattern = "stats_byhost.openapi_profile.sla_test.byhost.10_13_4_44.%s.%s.%s %s %s\n";

        int count = 0;

        while (++count <= 100000) {
            Socket socket = new Socket(host, port);

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), CharEncoding.UTF_8));

            String data = String.format(pattern, "dimension1", "dimension2", "metric1", "100", String.valueOf((System.currentTimeMillis() / 1000)));

            System.out.println(data);

            writer.write(data);
            writer.flush();

            writer.close();

            socket.close();

            Thread.sleep(1000);
        }
    }

}
