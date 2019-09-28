package com.weibo.dip.data.platform.datacubic.batch.aliyunossconfig;

/**
 * Created by xiaoyu on 2017/6/8.
 */
public class BatchSqlStr {

    public static final String sql = "SELECT\n" +
            "    _index,\n" +
            "    _type,\n" +
            "    _timestamp,\n" +
            "    SUM(length)/1073741824 AS sumlen,\n" +
            "    engineType\n" +
            "FROM (\n" +
            "    SELECT\n" +
            "        'dip-aliyunoss' AS _index,\n" +
            "        'v1' AS _type,\n" +
            "        time_to_utc_with_interval(CAST(ctime AS BIGINT), 'day') AS _timestamp,\n" +
            "        CAST(length AS DOUBLE) AS length,\n" +
            "        engineType\n" +
            "    FROM(\n" +
            "        SELECT\n" +
            "                fid,\n" +
            "                length,\n" +
            "                engineType,\n" +
            "                ctime\n" +
            "        FROM\n" +
            "                oss\n" +
            "        LATERAL VIEW json_tuple(content,\"fid\",\"length\",\"engineType\",\"ctime\") AS fid,length,engineType,ctime\n" +
            "        )tmp\n" +
            "    WHERE ctime IS NOT NULL\n" +
            "          AND ctime != ''\n" +
            "          AND length IS NOT NULL\n" +
            "    \t  AND length != ''\n" +
            "    \t  AND engineType IS NOT NULL\n" +
            "    \t  AND engineType != ''\n" +
            "    )tmp1\n" +
            "WHERE\n" +
            "        _timestamp IS NOT NULL\n" +
            "    AND _timestamp != ''\n" +
            "    AND length >= 0\n" +
            "GROUP BY _index, _type, _timestamp,engineType";

    public static final String sqlIp = "SELECT\n" +
            "\tserverip,\n" +
            "\tCOUNT(1) AS logsum\n" +
            "FROM(\n" +
            "\tSELECT \n" +
            "\t\tserverip\n" +
            "\tFROM\n" +
            "    \toss\n" +
            "    )tmp\n" +
            "GROUP BY serverip";

    public static final String sqlDate = "SELECT\n" +
            "     serverip,\n" +
            "     time,\n" +
            "     content\n" +
            "FROM(\n" +
            "\tSELECT \n" +
            "\t    serverip,\n" +
            "\t    content,\n" +
            "        CAST(ctime AS BIGINT) AS time\n" +
            "\tFROM \n" +
            "\t    oss\n" +
            "\tLATERAL VIEW json_tuple(content,\"fid\",\"length\",\"engineType\",\"ctime\") b AS fid,length,engineType,ctime\n" +
            ")\n" +
            "WHERE \n" +
            "    time> 1498665600000 AND time < 1498752000000";
}
