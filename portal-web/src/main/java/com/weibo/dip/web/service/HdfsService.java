package com.weibo.dip.web.service;

import java.util.Date;
import java.util.Map;
import org.apache.hadoop.fs.Path;

/**
 * Created by shixi_dongxue3 on 2017/12/14.
 */
public interface HdfsService {

  /**
   * 列出某目录下的所有文件.
   */
  Path[] listFiles(Path path);

  /**
   * 判断路径是否存在.
   */
  boolean exist(Path path);

  /**
   * 判断是否是目录.
   */
  boolean isDirectory(Path path);

  /**
   * 判断是否是文件.
   */
  boolean isFile(Path path);

  /**
   * 获取目标路径文件或目录大小.
   */
  float getFileSize(Path path);

  /**
   * 获得文件前100行的内容.
   */
  StringBuffer getContent(Path path);

  /**
   * 获得文件从修改到现在的时间.
   */
  Float getPeriod(Path path);

  /**
   * 删除Dir目录.
   */
  void deleteDir(Path path);

  /**
   * 根据日期范围返回某数据集下目录的大小的数组.
   */
  Map findByDatePeriod(Date startDate, Date endDate, String word);

  /**
   * 返回时间范围内各产品线的大小和总大小.
   */
  Map getSizeByProduct(Date startDate, Date endDate);
}
