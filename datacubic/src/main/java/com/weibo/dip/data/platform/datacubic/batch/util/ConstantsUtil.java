package com.weibo.dip.data.platform.datacubic.batch.util;

/**
 * Created by xiaoyu on 2017/3/13.
 */
public class ConstantsUtil {
    public static String[] COLUMNS = {"localerr_num","app_version","city","isp","neterr_num","local_time","during_time","net_time","system_version","total_num","network_type","succeed_num","timestamp"};

    public static final String OUTPUT_PATH = "/tmp/fulllink/2017_03_13/res";


    public static String MAPPER_NAME = "default";

    public static String SQL = "\n" +
            "SELECT\n" +
            "      timestamp, \n" +
            "      app_version, \n" +
            "      system_version, \n" +
            "      network_type, \n" +
            "      city, \n" +
            "      isp,\n" +
            "      SUM(total_num) AS total_num,\n" +
            "      SUM(succeed_num) AS succeed_num,\n" +
            "      SUM(neterr_num) AS neterr_num,\n" +
            "      SUM(localerr_num) AS localerr_num,\n" +
            "      SUM(during_time) AS during_time,\n" +
            "      SUM(net_time) AS net_time,\n" +
            "      SUM(local_time) AS local_time\n" +
            "FROM(\n" +
            "  SELECT\n" +
            "        *\n" +
            "        FROM\n" +
            "            (\n" +
            "                SELECT\n" +
            "                    timeAggregation(timestamp) as timestamp,\n" +
            "                    app_version,\n" +
            "                    city,\n" +
            "                    isp,\n" +
            "                    system_version,\n" +
            "                    network_type,\n" +
            "                    CAST(localerr_num AS BIGINT) AS localerr_num,\n" +
            "                    CAST(neterr_num AS BIGINT) AS neterr_num,\n" +
            "                    CAST(succeed_num AS BIGINT) AS succeed_num,\n" +
            "                    CAST(total_num AS BIGINT) AS total_num,\n" +
            "                    CAST(local_time AS DOUBLE) AS local_time,\n" +
            "                    CAST(during_time AS DOUBLE) AS during_time,\n" +
            "                    CAST(net_time AS DOUBLE) AS net_time\n" +
            "                FROM\n" +
            "                    fulllink\n" +
            "            ) temp\n" +
            "        WHERE\n" +
            "                    timestamp IS NOT NULL\n" +
            "                    AND app_version IS NOT NULL\n" +
            "                    AND app_version != ''\n" +
            "                    AND system_version IS NOT NULL\n" +
            "                    AND system_version != ''\n" +
            "                    AND network_type IS NOT NULL\n" +
            "                    AND network_type != ''\n" +
            "                    AND city IS NOT NULL\n" +
            "                    AND city != ''\n" +
            "                    AND isp IS NOT NULL\n" +
            "                    AND isp != ''\n" +
            "                    AND during_time >= 0\n" +
            "                    AND net_time >=0\n" +
            "                    AND localerr_num >= 0\n" +
            "                    AND neterr_num >= 0\n" +
            "                    AND succeed_num >=0 \n" +
            "                    AND total_num >= 0\n" +
            "                    AND local_time >= 0\n" +
            ") source\n" +
            "GROUP BY timestamp, app_version, system_version, network_type, city, isp";

}
