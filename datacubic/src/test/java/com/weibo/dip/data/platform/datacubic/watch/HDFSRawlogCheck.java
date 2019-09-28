package com.weibo.dip.data.platform.datacubic.watch;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by yurun on 17/1/24.
 */
public class HDFSRawlogCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRawlogCheck.class);

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");

        String url = "jdbc:mysql://10.13.56.31/dip?characterEncoding=UTF-8";
        String username = "aladdin";
        String password = "aladdin*admin";

        Connection conn = DriverManager.getConnection(url, username, password);

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select concat(c.type,'_',a.access_key,'_',c.dataset) as dataset from dip_categorys c inner join dip_access_key a on c.access_id = a.id;");

        Set<String> datasets = new HashSet<>();

        while (rs.next()) {
            datasets.add(rs.getString("dataset"));
        }

        rs.close();

        stmt.close();

        conn.close();

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        String result = "/user/hdfs/rawlog";

        FileStatus[] statuses = fs.listStatus(new Path(result));
        if (ArrayUtils.isEmpty(statuses)) {
            LOGGER.warn("statuses is empty");

            return;
        }

        Path[] datasetDirs = FileUtil.stat2Paths(statuses);

        for (Path datasetDir : datasetDirs) {
            String datasetName = datasetDir.getName();

            long size = fs.getContentSummary(datasetDir).getLength();

            if (!datasets.contains(datasetName)) {
                if (size <= 0) {
                    LOGGER.warn("Empty: " + datasetDir.toString() + " remove");

                    fs.delete(datasetDir, true);
                } else {
                    LOGGER.error("Not Empty: " + datasetDir.toString());

                    if (ArrayUtils.isNotEmpty(args) && args[0].equals("delete")) {
                        fs.delete(datasetDir, true);
                    }
                }
            }
        }
    }

}
