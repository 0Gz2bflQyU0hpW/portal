package com.weibo.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Created by yurun on 16/12/20.
 */
@SpringBootApplication
public class SpringBootHDFSClientApplication implements CommandLineRunner {

    @Autowired
    private HdfsService hdfsService;

    @Bean
    public HdfsService hdfsServiceProxy() {
        return ServiceProxyBuilder.buildLocalhost(HdfsService.class);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println(hdfsService.exist("/"));
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringBootHDFSClientApplication.class);

        application.setWebEnvironment(false);

        application.run(args);
    }

}
