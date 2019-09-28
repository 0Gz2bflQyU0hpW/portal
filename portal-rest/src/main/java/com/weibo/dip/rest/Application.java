package com.weibo.dip.rest;

import com.weibo.dip.rest.interceptor.RestAuthentificationInterceptor;
import com.weibo.dip.rest.listener.RestStartedListener;
import com.weibo.dip.rest.listener.RestStoppedListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Rest application.
 *
 * @author yurun
 */
@SpringBootApplication
public class Application extends WebMvcConfigurerAdapter {
  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new RestAuthentificationInterceptor()).addPathPatterns("/auth/**");
  }

  public static void main(String[] args) {
    SpringApplication application = new SpringApplication(Application.class);

    application.addListeners(new RestStartedListener());
    application.addListeners(new RestStoppedListener());

    application.run(args);
  }
}
