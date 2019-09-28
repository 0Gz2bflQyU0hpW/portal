package com.weibo.dip.data.platform.falcon.hdfs;

import com.weib.dip.data.platform.services.client.ElasticSearchService;
import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weib.dip.data.platform.services.client.model.IndexEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by yurun on 17/2/9.
 */
public class HDFSRawlogMonitor {

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

    private static Date getThreeDaysBefore(Date date) {
        Calendar calendar = Calendar.getInstance();

        calendar.setTime(date);

        calendar.add(Calendar.DAY_OF_YEAR, -3);

        return calendar.getTime();
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> datasetToProducts = getDatasetToProduct();

        HdfsService hdfsService = ServiceProxyBuilder.build(HdfsService.class);

        ElasticSearchService elasticSearchService = ServiceProxyBuilder.build(ElasticSearchService.class);

        try {
            Date now = new Date();

            SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy_MM_dd");

            String threeDaysBefore = dayFormat.format(getThreeDaysBefore(now));

            SimpleDateFormat indexDateFormat = new SimpleDateFormat("yyyyMMdd");

            String rawlog = "/user/hdfs/rawlog";

            Pattern datasetPattern = Pattern.compile("");

            Pattern dayPattern = Pattern.compile("\\d{4}_\\d{2}_\\d{2}");

            Pattern hourPattern = Pattern.compile("\\d{2}");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_ddHH");

            List<HFileStatus> datasetStatuses = hdfsService.listDirs(rawlog);
            if (CollectionUtils.isEmpty(datasetStatuses)) {
                LOGGER.error("Dir " + rawlog + " is empty");

                return;
            }

            for (HFileStatus datasetStatus : datasetStatuses) {
                String dataset = datasetStatus.getName();
                if (!datasetToProducts.containsKey(dataset)) {
                    LOGGER.error("Dataset " + dataset + " not exist in db");

                    continue;
                }

                List<HFileStatus> dayStatuses = hdfsService.listDirs(datasetStatus.getPath());
                if (CollectionUtils.isEmpty(dayStatuses)) {
                    LOGGER.error("Dir " + datasetStatus.getPath() + " is empty");

                    continue;
                }

                for (HFileStatus dayStatus : dayStatuses) {
                    String day = dayStatus.getName();
                    if (!dayPattern.matcher(day).matches()) {
                        LOGGER.error("Day " + day + " not match");

                        continue;
                    }

                    if (day.compareTo(threeDaysBefore) <= 0) {
                        continue;
                    }

                    List<HFileStatus> hourStatuses = hdfsService.listDirs(dayStatus.getPath());
                    if (CollectionUtils.isEmpty(hourStatuses)) {
                        LOGGER.error("Dir " + dayStatus.getPath() + " is empty");

                        continue;
                    }

                    for (HFileStatus hourStatus : hourStatuses) {
                        String hour = hourStatus.getName();
                        if (!hourPattern.matcher(hour).matches()) {
                            LOGGER.error("Hour " + hour + " not match");

                            continue;
                        }

                        String product = datasetToProducts.get(dataset);

                        long length = hdfsService.getLength(hourStatus.getPath());

                        Date timestamp = dateFormat.parse(day + hour);

                        String id = String.join("_", product, dataset, day, hour);

                        IndexEntity entity = new IndexEntity();

                        entity.setIndex("dip-monitoring-hdfs-" + indexDateFormat.format(timestamp));
                        entity.setType("rawlog");

                        entity.setId(id);

                        entity.setTerm("product", product);
                        entity.setTerm("dataset", dataset);
                        entity.setTerm("day", day);
                        entity.setTerm("hour", hour);
                        entity.setTerm("length", length);

                        entity.setTimestamp(timestamp);

                        elasticSearchService.index(entity);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("hdfs rawlog monitor error: " + ExceptionUtils.getFullStackTrace(e));
        }

    }

}
