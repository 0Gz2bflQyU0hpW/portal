package com.weibo.dip.web.service.impl;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import com.weibo.dip.web.service.HdfsService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by shixi_dongxue3 on 2017/12/14.
 */
public class HdfsServiceImpl implements HdfsService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsServiceImpl.class);

  @Override
  public Path[] listFiles(Path path) {
    FileSystem hdfs = HDFSUtil.getFileSystem();
    FileStatus[] fs = new FileStatus[0];

    try {
      fs = hdfs.listStatus(path);
    } catch (IOException e) {
      LOGGER.error("get listStatus under dir error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    Path[] paths = FileUtil.stat2Paths(fs);
    return paths;
  }

  @Override
  public float getFileSize(Path path) {
    FileSystem fs = HDFSUtil.getFileSystem();
    float size = 0;
    try {
      size = fs.getContentSummary(path).getLength();
    } catch (IOException e) {
      LOGGER.error("get size of path:{}", ExceptionUtils.getFullStackTrace(e));
    }

    return size / 1024 / 1024;       //转换成M
  }

  @Override
  public StringBuffer getContent(Path path) {
    StringBuffer txtContent = new StringBuffer();
    FSDataInputStream fin = null;

    try {
      fin = HDFSUtil.openInputStream(path);
    } catch (IOException e) {
      LOGGER.error("get content of file error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    BufferedReader in = null;
    String line;
    int i = 0;
    try {
      in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
      while ((line = in.readLine()) != null && i < 100) {           //拿到前100行的数据
        txtContent.append(new String(line));
        txtContent.append(new String("\n"));
        i++;
      }
    } catch (IOException e) {
      LOGGER.error("get inputStream of file error:{}", ExceptionUtils.getFullStackTrace(e));
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          LOGGER.error("close inputStream error:{}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }
    return txtContent;
  }

  @Override
  public boolean exist(Path path) {
    try {
      return HDFSUtil.exist(path);
    } catch (IOException e) {
      LOGGER.error("get the existence of path error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return false;
  }

  @Override
  public boolean isDirectory(Path path) {
    try {
      return HDFSUtil.isDirectory(path);
    } catch (IOException e) {
      LOGGER.error("get isDir of path error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return false;
  }

  @Override
  public boolean isFile(Path path) {
    try {
      return HDFSUtil.isFile(path);
    } catch (IOException e) {
      LOGGER.error("get isFile of path error:{}", ExceptionUtils.getFullStackTrace(e));
    }
    return false;
  }

  @Override
  public Float getPeriod(Path path) {
    FileSystem fs = HDFSUtil.getFileSystem();
    FileStatus fileStatus = new FileStatus();
    try {
      fileStatus = fs.getFileStatus(path);
    } catch (IOException e) {
      LOGGER.error("get fileStatus of file error:{}", ExceptionUtils.getFullStackTrace(e));
    }

    Date modifyTime = new Date(fileStatus.getModificationTime());
    Date now = new Date();

    //获取从修改到现在的时间差，毫秒转换成天
    float period = (now.getTime() - modifyTime.getTime()) / 1000 / 60 / 60 / 24;

    return period;
  }

  @Override
  public void deleteDir(Path path) {
    try {
      HDFSUtil.deleteDir(path, true);
    } catch (IOException e) {
      LOGGER.error("delete dir error:{}", ExceptionUtils.getFullStackTrace(e));
    }
  }

  @Override
  public Map findByDatePeriod(Date startDate, Date endDate, String word) {
    final Path rawlogPrefix = new Path("/user/hdfs/rawlog");
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd HH:mm:ss");
    SimpleDateFormat dateFormatDayPath = new SimpleDateFormat("yyyy_MM_dd");

    Map<String, Object> map = new HashMap<>();
    Map<String, List> mapTime = new HashMap<>();
    Map<String, List> mapSize = new HashMap<>();
    Set<String> nameList = new HashSet<>();

    Path[] listPath = listFiles(rawlogPrefix);
    //遍历每个数据集
    for (Path datasetPath : listPath) {
      Path[] datasetDayPath = listFiles(datasetPath);
      List<Date> time = new ArrayList<>();
      List<Float> size = new ArrayList<>();
      if (!StringUtils.isBlank(word)
          && word.equals(datasetPath.getName())
          || StringUtils.isBlank(word)) {
        //遍历某个数据集下日期目录
        for (int i = 0; i < datasetDayPath.length; i++) {
          //如果日期目录在搜索的时间区间内，遍历子目录
          try {
            if (dateFormatDayPath.parse(datasetDayPath[i].getName()).getTime() >= startDate
                .getTime()
                && dateFormatDayPath.parse(datasetDayPath[i].getName()).getTime() <= endDate
                .getTime()) {
              Path[] datasetHourPath = listFiles(datasetDayPath[i]);
              for (int j = 0; j < datasetHourPath.length; j++) {
                //如果小时目录在搜索的时间区间内，将数据集名称、每个时间点和以及对应的大小装入集合
                if (dateFormat.parse(
                    datasetDayPath[i].getName() + " " + datasetHourPath[j].getName() + ":00:00")
                    .getTime() >= startDate.getTime() && dateFormat.parse(
                    datasetDayPath[i].getName() + " " + datasetHourPath[j].getName() + ":00:00")
                    .getTime() <= endDate.getTime()) {
                  nameList.add(datasetPath.getName());
                  Date date = dateFormat.parse(
                      datasetDayPath[i].getName() + " " + datasetHourPath[j].getName() + ":00:00");
                  time.add(date);
                  size.add(getFileSize(datasetHourPath[j]));
                }
              }
            }
          } catch (ParseException e) {
            LOGGER.error("turn from path to date error:{}", ExceptionUtils.getFullStackTrace(e));
          }
        }

        mapTime.put(datasetPath.getName(), time);
        mapSize.put(datasetPath.getName(), size);
        if (!StringUtils.isBlank(word)) {
          break;
        }
      }
    }

    map.put("nameList", nameList);
    map.put("mapTime", mapTime);
    map.put("mapSize", mapSize);

    return map;
  }

  @Override
  public Map getSizeByProduct(Date startDate, Date endDate) {
    final Path rawlogPrefix = new Path("/user/hdfs/rawlog");
    SimpleDateFormat dateFormatDayPath = new SimpleDateFormat("yyyy_MM_dd");

    Map<String, Object> map = new HashMap<>();
    Map<String, Float> mapProduct = new HashMap<>();
    Statement stmt = getDatasetIndb();
    Float sum = 0f;

    Path[] listPath = listFiles(rawlogPrefix);
    //遍历每个数据集，得到每个产品线的大小
    for (Path datasetPath : listPath) {
      Path[] datasetDayPath = listFiles(datasetPath);
      String product = getProductByName(stmt, datasetPath.getName());
      //遍历某个数据集下日期目录
      for (int i = 0; i < datasetDayPath.length; i++) {
        //如果日期目录在搜索的时间区间内，遍历子目录
        try {
          if (dateFormatDayPath.parse(datasetDayPath[i].getName()).getTime() >= startDate.getTime()
              && dateFormatDayPath.parse(datasetDayPath[i].getName()).getTime() <= endDate
              .getTime()) {
            if (mapProduct.containsKey(product)) {
              mapProduct.put(product, mapProduct.get(product) + getFileSize(datasetDayPath[i]));
            } else {
              mapProduct.put(product, getFileSize(datasetDayPath[i]));
            }
            sum += getFileSize(datasetPath);
          }
        } catch (ParseException e) {
          LOGGER.error("turn from day dir to date error:{}", ExceptionUtils.getFullStackTrace(e));
        }
      }
    }

    map.put("mapProduct", mapProduct);
    map.put("sum", sum);

    return map;
  }

  /*获取数据库连接*/
  private Statement getDatasetIndb() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    Connection conn;
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

  /*查询名称为datasetName的数据集的产品线*/
  private String getProductByName(Statement stmt, String datasetName) {
    ResultSet rs = null;
    String product = null;

    try {
      rs = stmt
          .executeQuery("select product from dataset "
              + "where dataset_name='" + datasetName + "'");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      rs.next();
      product = rs.getString("product");
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return product;
  }


}
