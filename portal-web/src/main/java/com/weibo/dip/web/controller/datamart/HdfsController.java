package com.weibo.dip.web.controller.datamart;

import com.weibo.dip.web.model.datamart.HdfsFile;
import com.weibo.dip.web.service.DatasetService;
import com.weibo.dip.web.service.HdfsService;
import com.weibo.dip.web.service.impl.HdfsServiceImpl;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@RequestMapping("/datamart/hdfs")
public class HdfsController {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsController.class);
  private HdfsService hdfsService = new HdfsServiceImpl();
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  @Autowired
  private DatasetService datasetService;

  /**
   * 获取目录列表.
   *
   * @param path1 路径参数
   * @return 返回查询结果，具体含义再说明
   */
  @RequestMapping("/listFile")
  @ResponseBody
  public Map listFile(@RequestParam("path") String path1) {
    Path path = new Path(path1);
    Map map = new HashMap();

    if (hdfsService.exist(path)) {
      map.put("exist", true);
      if (hdfsService.isDirectory(path)) {
        Path[] listPath = hdfsService.listFiles(path);
        List<HdfsFile> hdfsFiles = new ArrayList<>();

        for (int i = 0; i < listPath.length; i++) {
          HdfsFile hdfsFile = new HdfsFile();
          hdfsFile.setHdfsFilePath(listPath[i]);
          hdfsFile.setHdfsFileName(listPath[i].getName());
          hdfsFile.setHdfsFilesize(hdfsService.getFileSize(listPath[i]));

          hdfsFiles.add(hdfsFile);
        }
        map.put("isDir", true);
        map.put("hdfsFiles", hdfsFiles);
      } else if (hdfsService.isFile(path)) {

        map.put("isDir", false);
      } else {
        LOGGER.error("can get path is dir or file");
      }
    } else {
      map.put("exist", false);
      LOGGER.error("path is not exist or the format is incorrect");
    }

    return map;
  }

  /**
   * 获取文件内容.
   *
   * @param path1 路径参数
   * @return 返回查询结果，具体含义再说明
   */
  @RequestMapping("/getFileContent")
  @ResponseBody
  public Map getFileContent(@RequestParam("path") String path1) {
    Path path = new Path(path1);
    Map map = new HashMap();
    map.put("content", hdfsService.getContent(path));

    return map;
  }

  /**
   * 画折线图.
   *
   * @param startDate 开始日期
   * @param endDate 结束日期
   * @param word 显示文字
   * @return 返回结果
   */
  @RequestMapping("/getLine")
  @ResponseBody
  public Map getLine(@RequestParam("startDate") String startDate,
      @RequestParam("endDate") String endDate, @RequestParam("word") String word) {
    Map map = new HashMap();

    if (StringUtils.isBlank(startDate)) {
      startDate = "2000-01-01 00:00:00";
    }
    if (StringUtils.isBlank(endDate)) {
      endDate = "2500-01-01 00:00:00";
    }

    try {
      map = hdfsService
          .findByDatePeriod(dateFormat.parse(startDate), dateFormat.parse(endDate), word);
    } catch (Exception e) {
      LOGGER.error("false of String to Date:{}", ExceptionUtils.getFullStackTrace(e));
    }

    String[] nameList = (String[]) ((HashSet) map.get("nameList"))
        .toArray(new String[((HashSet) map.get("nameList")).size()]);
    Float[][] size = new Float[nameList.length][];
    Date[][] time = new Date[nameList.length][];

    for (int i = 0; i < nameList.length; i++) {
      size[i] = (Float[]) ((ArrayList) ((Map) map.get("mapSize")).get(nameList[i]))
          .toArray(new Float[((ArrayList) ((Map) map.get("mapSize")).get(nameList[i])).size()]);
      time[i] = (Date[]) ((ArrayList) ((Map) map.get("mapTime")).get(nameList[i]))
          .toArray(new Date[((ArrayList) ((Map) map.get("mapTime")).get(nameList[i])).size()]);
    }
    Map mapSend = new HashMap();
    mapSend.put("nameList", nameList);
    mapSend.put("size", size);
    mapSend.put("time", time);

    return mapSend;
  }

  /**
   * 画饼图.
   *
   * @param startDate 开始时间
   * @param endDate 结束时间
   * @return 返回结果
   */
  @RequestMapping("/getPie")
  @ResponseBody
  public Map getPie(@RequestParam("startDate") String startDate,
      @RequestParam("endDate") String endDate) {
    Map map = new HashMap();

    try {
      map = hdfsService.getSizeByProduct(dateFormat.parse(startDate), dateFormat.parse(endDate));
    } catch (Exception e) {
      LOGGER.error("false of String to Date:{}", ExceptionUtils.getFullStackTrace(e));
    }

    Set products = ((HashMap) map.get("mapProduct")).keySet();
    String[] productList = new String[products.size()];

    int i = 0;
    for (Iterator iterator = products.iterator(); iterator.hasNext(); i++) {
      productList[i] = (String) iterator.next();
    }
    map.put("productList", productList);

    return map;
  }

}
