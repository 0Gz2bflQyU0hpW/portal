package com.weibo.dip.data.platform.services.server;

import com.weib.dip.data.platform.services.client.*;
import com.weibo.dip.data.platform.services.server.util.ServiceExporterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.remoting.caucho.HessianServiceExporter;

/**
 * Created by yurun on 16/11/29.
 */
@SpringBootApplication
public class ServicesApplication {

    @Autowired
    private ConfigService configService;

    @Autowired
    private HdfsService hdfsService;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private AlarmService alarmService;

    @Autowired
    private ElasticSearchService elasticSearchService;

    @Autowired
    private DatasetService datasetService;

    @Bean(name = "/services/configService")
    public HessianServiceExporter configService() {
        return ServiceExporterBuilder.build(configService, ConfigService.class);
    }

    @Bean(name = "/services/hdfsService")
    public HessianServiceExporter hdfsService() {
        return ServiceExporterBuilder.build(hdfsService, HdfsService.class);
    }

    @Bean(name = "/services/yarnService")
    public HessianServiceExporter yarnService() {
        return ServiceExporterBuilder.build(yarnService, YarnService.class);
    }

    @Bean(name = "/services/alarmService")
    public HessianServiceExporter alarmService() {
        return ServiceExporterBuilder.build(alarmService, AlarmService.class);
    }

    @Bean(name = "/services/elasticSearchService")
    public HessianServiceExporter elasticSearchService() {
        return ServiceExporterBuilder.build(elasticSearchService, ElasticSearchService.class);
    }
    @Bean(name = "/services/datasetService")
    public HessianServiceExporter datasetService() {
        return ServiceExporterBuilder.build(datasetService, DatasetService.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(ServicesApplication.class, args);
    }

}
