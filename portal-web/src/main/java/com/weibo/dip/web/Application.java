package com.weibo.dip.web;

import com.weibo.dip.web.interceptor.WebAuthentificationInterceptor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@SpringBootApplication
public class Application extends WebMvcConfigurerAdapter {

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new WebAuthentificationInterceptor()).addPathPatterns("/**")
        .excludePathPatterns("/cas/*");
  }

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }
}
