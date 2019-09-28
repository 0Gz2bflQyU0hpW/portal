package com.weibo.dip.web.service;

import com.weibo.dip.web.service.impl.HdfsServiceImpl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


/**
 * Created by shixi_dongxue3 on 2017/12/26.
 */
@Component
@EnableScheduling
public class HdfsScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsScheduler.class);
  private static final Path RAWLOG_PREFIX = new Path("/user/hdfs/rawlog");
  SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private HdfsService hdfsService = new HdfsServiceImpl();
  /*@Scheduled(fixedRate = 1000 * 60 * 10)
    private void OverStorePeriod(){
    }*/

  /**
   * 每小时执行一次.
   */
  @Scheduled(cron = "0 0 0/1 * * ? ")
  public void hdfsScheduler() {
    Statement stmt = getDatasetIndb();
    Date date = new Date();
    LOGGER.info("the current time:" + timeFormat.format(date));

    Path[] listPath = hdfsService.listFiles(RAWLOG_PREFIX);

    for (Path datasetPath : listPath) {
      LOGGER.info("begin path：" + datasetPath);
      //如果数据集在mysql中不存在，将其删除
      if (getNameNum(stmt, datasetPath.getName()) == 0) {
        LOGGER.info("delete the dataset which not in mysql:" + datasetPath.getName());
        //hdfsService.deleteDir(datasetPath);
      } else {
        Path[] datasetDayPath = hdfsService.listFiles(datasetPath);
        int storePeriod = (int) findDatasetByName(stmt, datasetPath.getName()).get("storePeriod");

        /*遍历每个数据集下的目录，将过期的删除*/
        for (int i = 0; i < datasetDayPath.length; i++) {
          LOGGER.info("begin path:" + datasetDayPath[i]);
          LOGGER.info("already stored time" + hdfsService.getPeriod(datasetDayPath[i]));
          LOGGER.info("storePeriod:" + storePeriod);
          if (hdfsService.getPeriod(datasetDayPath[i]) >= storePeriod) {
            LOGGER.info("delete dir beyond time" + datasetDayPath[i]);
            //hdfsService.deleteDir(datasetDayPath[i]);
          }
        }

        /*如果hdfs中所存的某数据集总大小超过size则发出报警*/
        LOGGER
            .info(datasetPath + "size of dir:" + hdfsService.getFileSize(datasetPath) / 1024 + "G");
        LOGGER.info("size in mysql：" + findDatasetByName(stmt, datasetPath.getName()).get("size"));
        if (hdfsService.getFileSize(datasetPath) / 1024 > (float) findDatasetByName(stmt,
            datasetPath.getName()).get("size")) {
          LOGGER.info(
              datasetPath.getName() + "size of dataset:" + hdfsService.getFileSize(datasetPath)
                  + "M，beyond the size in mysql:" + (float) findDatasetByName(stmt,
                  datasetPath.getName()).get("size") + "G");
        }
      }
    }
  }

  /*获取数据库连接*/
  private Statement getDatasetIndb() {
    Map<String, String> datasetToProducts = new HashMap<>();

    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    Connection conn = null;
    Statement stmt = null;

    try {
      conn = DriverManager.getConnection(
          "jdbc:mysql://d136092.innet.dip.weibo.com:3307/"
              + "portal?characterEncoding=UTF-8&useSSL=false",
          "root", "mysqladmin");
      stmt = conn.createStatement();
    } catch (Exception e) {
      LOGGER.error("select from db error: " + ExceptionUtils.getFullStackTrace(e));
    }
    return stmt;
  }

  /*查询名称为datasetName的数据集在db中是否存在，存在返回1，不存在返回0*/
  private int getNameNum(Statement stmt, String datasetName) {
    ResultSet rs = null;
    int num = 0;

    try {
      rs = stmt
          .executeQuery("select count(*) from dataset where dataset_name='" + datasetName + "'");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      rs.next();
      num = rs.getInt(1);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return num;
  }

  /*查询名称为datasetName的数据集的size和storePeriod*/
  private Map findDatasetByName(Statement stmt, String datasetName) {
    ResultSet rs = null;
    int storePeriod = 0;
    float size = 0;
    Map map = new HashMap();

    try {
      rs = stmt.executeQuery("select * from dataset where dataset_name='" + datasetName + "'");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      rs.next();
      storePeriod = rs.getInt("store_period");
      size = rs.getFloat("size");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    map.put("storePeriod", storePeriod);
    map.put("size", size);
    return map;
  }
}
