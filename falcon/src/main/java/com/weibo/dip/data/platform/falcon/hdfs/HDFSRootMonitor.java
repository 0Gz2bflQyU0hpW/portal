package com.weibo.dip.data.platform.falcon.hdfs;

import com.weib.dip.data.platform.services.client.ElasticSearchService;
import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.model.IndexEntity;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yurun on 17/2/9.
 */
public class HDFSRootMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRootMonitor.class);

    public static void main(String[] args) {
        HdfsService hdfsService = ServiceProxyBuilder.build(HdfsService.class);

        ElasticSearchService elasticSearchService = ServiceProxyBuilder.build(ElasticSearchService.class);

        try {
            Date now = new Date();

            SimpleDateFormat indexPattern = new SimpleDateFormat("yyyyMMdd");

            long length = hdfsService.getLength("/");

            IndexEntity entity = new IndexEntity();

            entity.setIndex("dip-monitoring-hdfs-" + indexPattern.format(now));
            entity.setType("root");

            entity.setTerm("length", length);

            entity.setTimestamp(now);

            elasticSearchService.index(entity);
        } catch (Exception e) {
            LOGGER.error("hdfs root monitor error: " + ExceptionUtils.getFullStackTrace(e));
        }

    }

}
