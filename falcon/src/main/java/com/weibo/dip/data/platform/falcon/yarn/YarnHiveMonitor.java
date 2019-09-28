package com.weibo.dip.data.platform.falcon.yarn;

import com.weib.dip.data.platform.services.client.ElasticSearchService;
import com.weib.dip.data.platform.services.client.model.IndexEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import com.weibo.dip.data.platform.falcon.hdfs.HDFSRawlogMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/2/9.
 */
public class YarnHiveMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRawlogMonitor.class);

    private static Map<String, String> getDatasetToProduct() throws Exception {
        Map<String, String> datasetToProducts = new HashMap<>();

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;

        Statement stmt = null;

        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://10.13.56.31:3306/dip?useUnicode=true&characterEncoding=UTF8", "aladdin", "aladdin*admin");

            stmt = conn.createStatement();

            rs = stmt.executeQuery("select concat(type, '_', a.access_key, '_', dataset) as dataset, product from dip_categorys c inner join dip_access_key a on c.access_id = a.id");

            while (rs.next()) {
                datasetToProducts.put(rs.getString("dataset"), rs.getString("product"));
            }
        } catch (Exception e) {
            LOGGER.error("select from db error: " + ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.error("rs close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    LOGGER.error("stmt close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("conn close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

        return datasetToProducts;
    }

    private static String getYestoday(Date now) {
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(now);

        calendar.add(Calendar.DAY_OF_YEAR, -1);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        return dateFormat.format(calendar.getTime());
    }

    private static class Record {

        private String jobname;

        private String dataset;

        private String product;

        private long length;

        private Date timestamp;

        public Record(String jobname, String dataset, String product, long length, Date timestamp) {
            this.jobname = jobname;
            this.dataset = dataset;
            this.product = product;
            this.length = length;
            this.timestamp = timestamp;
        }

        public String getJobname() {
            return jobname;
        }

        public String getDataset() {
            return dataset;
        }

        public String getProduct() {
            return product;
        }

        public long getLength() {
            return length;
        }

        public Date getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Record{" +
                "jobname='" + jobname + '\'' +
                ", dataset='" + dataset + '\'' +
                ", product='" + product + '\'' +
                ", length=" + length +
                ", timestamp=" + timestamp +
                '}';
        }

    }

    private static List<Record> getRecords(String day) throws Exception {
        String starttime = day + " 00:00:00";
        String endtime = day + " 23:59:59";

        Pattern hiveNamePattern = Pattern.compile("hive_(.*)_\\d+");
        Pattern selectjobNamePattern = Pattern.compile("select_job_(.*)_\\d+-.*");

        List<Record> records = new ArrayList<>();

        Map<String, String> datasetToProducts = getDatasetToProduct();

        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;

        Statement stmt = null;

        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://m6103i.eos.grid.sina.com.cn:6103/dip_data_analyze?useUnicode=true&characterEncoding=UTF8", "dipadmin", "dipqwe123");

            stmt = conn.createStatement();

            rs = stmt.executeQuery("select jobname, category, hdfs_bytes_read as length, c_time as timestamp from dip_mr_log where c_time >= '" + starttime + "' and c_time <= '" + endtime + "' order by c_time desc");

            while (rs.next()) {
                String jobname = rs.getString("jobname");

                if (jobname.startsWith("hive")) {
                    Matcher matcher = hiveNamePattern.matcher(jobname);

                    if (!matcher.matches()) {
                        continue;
                    }

                    jobname = matcher.group(1);
                } else if (jobname.startsWith("select_job")) {
                    Matcher matcher = selectjobNamePattern.matcher(jobname);

                    if (!matcher.matches()) {
                        continue;
                    }

                    jobname = matcher.group(1);
                } else {
                    continue;
                }

                String category = rs.getString("category");

                if (StringUtils.isEmpty(category)) {
                    continue;
                }

                category = category.split("\\|")[0];

                if (!datasetToProducts.containsKey(category)) {
                    continue;
                }

                long length = rs.getLong("length");

                Date timestamp = new Date(rs.getTimestamp("timestamp").getTime());

                records.add(new Record(jobname, category, datasetToProducts.get(category), length, timestamp));
            }
        } catch (Exception e) {
            LOGGER.error("select from db error: " + ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.error("rs close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    LOGGER.error("stmt close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.error("conn close error: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }

        return records;
    }

    public static void main(String[] args) throws Exception {
        Date now = new Date();

        String day = getYestoday(now);

        if (ArrayUtils.isNotEmpty(args)) {
            day = args[0];
        }

        List<Record> records = getRecords(day);

        if (CollectionUtils.isEmpty(records)) {
            LOGGER.error("hive records is empty");

            return;
        }

        ElasticSearchService elasticSearchService = ServiceProxyBuilder.build(ElasticSearchService.class);

        SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMMdd");

        for (Record record : records) {
            IndexEntity entity = new IndexEntity();

            entity.setIndex("dip-monitoring-yarn-" + indexDateFormat.format(record.getTimestamp()));
            entity.setType("hive");

            entity.setId(String.join("_", record.getProduct(), record.getDataset(), record.getJobname(), record.getTimestamp().toString()));

            entity.setTerm("product", record.getProduct());
            entity.setTerm("dataset", record.getDataset());
            entity.setTerm("jobname", record.getJobname());
            entity.setTerm("length", record.getLength());

            entity.setTimestamp(record.getTimestamp());

            elasticSearchService.index(entity);
        }
    }

}
