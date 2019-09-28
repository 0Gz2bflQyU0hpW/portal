package com.weibo.dip.data.platform.datacubic.live;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/2/15.
 */
public class LiveRegexMain {

    public static void main(String[] args) throws Exception {
        String timestamp = "2017-02-07 15:18:54";

        String ip = "58.214.240.114";

        String sessionid = "1486451934_2128611862";

        String action = "join_room";

        String uid = "2128611862";

        String clientfrom = "1070093010";

        String roomid = "1042097:81aca7bc94c27c5d6dad6e13a151f212";

        String containerid = "23091681aca7bc94c27c5d6dad6e13a151f212";

        String status = "1";

        String extend = "play_type=>live,online_num=>2\n";

        String line = String.join("\t", timestamp, ip, sessionid, action, uid, clientfrom, roomid, containerid, status, extend);

        String regex = "^([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)$";

        Properties properties = new Properties();

        properties.load(LiveRegexMain.class.getClassLoader().getResourceAsStream("live_regex.config"));

        regex = properties.getProperty("source.kafka.topic.regex");

        System.out.println("Regex:" + regex);

        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(line);

        if (matcher.matches()) {
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
        }
    }

}
