package com.weibo.dip.costing.config;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Created by yurun on 18/4/18.
 */
@Configuration
public class DataSourceConfig {

  @Bean(name = "primaryDataSource")
  @ConfigurationProperties(prefix = "spring.datasource.primary")
  @Primary
  public DataSource primaryDataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "primaryJdbcTemplate")
  @Primary
  public JdbcTemplate primaryJdbcTemplate(@Qualifier("primaryDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  @Bean(name = "costDataSource")
  @ConfigurationProperties(prefix = "spring.datasource.cost")
  public DataSource costDataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "costJdbcTemplate")
  public JdbcTemplate costJdbcTemplate(@Qualifier("costDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  @Bean(name = "consoleDataSource")
  @ConfigurationProperties(prefix = "spring.datasource.console")
  public DataSource consoleDataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "consoleJdbcTemplate")
  public JdbcTemplate consoleJdbcTemplate(@Qualifier("consoleDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  @Bean(name = "testDataSource")
  @ConfigurationProperties(prefix = "spring.datasource.test")
  public DataSource testDataSource() {
    return DataSourceBuilder.create().build();
  }

  @Bean(name = "testJdbcTemplate")
  public JdbcTemplate testJdbcTemplate(@Qualifier("testDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

}
