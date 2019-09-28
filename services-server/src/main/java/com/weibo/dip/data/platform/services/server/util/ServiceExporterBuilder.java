package com.weibo.dip.data.platform.services.server.util;

import org.springframework.remoting.caucho.HessianServiceExporter;

/**
 * Created by yurun on 16/12/13.
 */
public class ServiceExporterBuilder {

    private ServiceExporterBuilder() {

    }

    public static HessianServiceExporter build(Object service, Class<?> clazz) {
        HessianServiceExporter exporter = new HessianServiceExporter();

        exporter.setService(service);
        exporter.setServiceInterface(clazz);

        return exporter;
    }

}
