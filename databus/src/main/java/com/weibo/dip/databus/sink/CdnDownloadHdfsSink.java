package com.weibo.dip.databus.sink;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.databus.core.Configuration;
import com.weibo.dip.databus.core.Constants;
import com.weibo.dip.databus.core.Message;
import com.weibo.dip.databus.core.Sink;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by jianhong1 on 2018/5/31.
 */
public class CdnDownloadHdfsSink extends Sink {
  private static final Logger LOGGER = LoggerFactory.getLogger(CdnDownloadHdfsSink.class);
  // HDFS_BASE_DIR/category/day_and_hour/logName
  private static final String FILE_PATH_PATTERN = "%s/%s/%s/%s";
  private static final String HDFS_BASE_DIR = "sink.hdfs.base.dir";
  private static final String CATEGORY = "Category";
  private static final String FILE_NAME = "FileName";
  private static final String FILE_URL = "FileUrl";
  private static final String TIMESTAMP = "Timestamp";
  private static final int DEFAULT_TIMEOUT = 10000;
  private static final int RETRY_TIME = 3;
  private static FileSystem filesystem;
  private static FastDateFormat filePathDateFormat = FastDateFormat.getInstance("yyyy_MM_dd/HH");

  static {
    try {
      filesystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
    } catch (IOException e) {
      throw new ExceptionInInitializerError("init hdfs filesystem error: " + e);
    }
  }

  private String hdfsBaseDir;

  @Override
  public void process(Message message) throws Exception {
    String line = message.getData();
    if (StringUtils.isEmpty(line)) {
      return;
    }
    long startTime = System.currentTimeMillis();

    Map<String, Object> map = GsonUtil.fromJson(line, GsonUtil.GsonType.OBJECT_MAP_TYPE);
    String category = map.get(CATEGORY).toString();
    String originFileName = map.get(FILE_NAME).toString();
    String fileUrl = map.get(FILE_URL).toString();
    long timestamp = ((Double) map.get(TIMESTAMP)).longValue();

    String fileName = originFileName;
    String codec = null;

    if (".gz".equals(originFileName.substring(originFileName.length() - 3, originFileName.length()))) {
      fileName = originFileName.substring(0, originFileName.length() - 3) + ".log";
      codec = "gz";
    } else {
      LOGGER.warn("http file:{} is not gz file", originFileName);
    }

    String dayHour = filePathDateFormat.format(timestamp);
    String dotFileName = "." + fileName;
    String srcFilePath = String
        .format(FILE_PATH_PATTERN, hdfsBaseDir, category, dayHour, dotFileName);
    String dstFilePath = String
        .format(FILE_PATH_PATTERN, hdfsBaseDir, category, dayHour, fileName);

    Path dstPath = new Path(dstFilePath);
    Path srcPath = new Path(srcFilePath);

    //若下载失败,最多重试三次
    for (int time = 0; time < RETRY_TIME; time++) {
      if (time != 0) {
        LOGGER.info("download retry time: {}", time + 1);
      }
      try {
        downloadToHdfs(fileUrl, srcPath, codec);
        break;
      } catch (Exception e) {
        if(filesystem.exists(srcPath)){
          filesystem.delete(srcPath, true);
        }
        LOGGER.warn("{} download fail, deleted hdfs temp file: {} \n{}", fileUrl, srcFilePath, ExceptionUtils.getFullStackTrace(e));
      }
    }

    try {
      if(!filesystem.exists(srcPath)){
        long endTime = System.currentTimeMillis();
        LOGGER.error("{} download fail, spent time: {}s", fileUrl, (endTime-startTime)/1000);
        return;
      }

      if (filesystem.exists(dstPath)) {
        filesystem.delete(dstPath, true);
        LOGGER.info("before rename dotFile to file, file has existed, delete it: {}", dstFilePath);
      }

      filesystem.rename(srcPath, dstPath);
      long endTime = System.currentTimeMillis();
      LOGGER.info("{} download and rename success, hdfs path: {}, spent time: {}s", fileUrl, dstFilePath, (endTime-startTime)/1000);
    } catch (Exception e) {
      LOGGER.error("rename {} to {} error \n{}", srcPath, dstPath, ExceptionUtils.getFullStackTrace(e));
    }
  }

  private void downloadToHdfs(String fileUrl, Path srcPath, String codec) throws Exception {
    FSDataOutputStream outputStream = null;
    InputStream inputStream = null;
    GetMethod method = null;

    HttpClient httpClient = new HttpClient();
    httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(DEFAULT_TIMEOUT);
    httpClient.getHttpConnectionManager().getParams().setSoTimeout(DEFAULT_TIMEOUT);
    try {
      outputStream = filesystem.create(srcPath, true);

      method = new GetMethod(fileUrl);
      method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
          new DefaultHttpMethodRetryHandler(RETRY_TIME, false));

      int responseCode = httpClient.executeMethod(method);

      if(responseCode != HttpStatus.SC_OK){
        LOGGER.error("download {} executeMethod failed: {}", fileUrl, method.getStatusLine());
        return;
      }

      long contentLength = method.getResponseContentLength();
      LOGGER.info("start downloading:{}, GetMethod ContentLength:{}, executeMethod ResponseCode:{}",
          fileUrl, contentLength, responseCode);

      if ("gz".equals(codec)) {
        inputStream = new GZIPInputStream(method.getResponseBodyAsStream());
      } else {
        inputStream = method.getResponseBodyAsStream();
      }

      IOUtils.copyBytes(inputStream, outputStream, 4096);
      outputStream.flush();
      LOGGER.info("{} download success, hdfs path:{}", fileUrl, srcPath);
    } finally {
      IOUtils.closeStream(inputStream);
      IOUtils.closeStream(outputStream);
      if (method != null) {
        method.releaseConnection();
      }
    }
  }

  @Override
  public void setConf(Configuration conf) {
    name = conf.get(Constants.PIPELINE_NAME) + Constants.HYPHEN + this.getClass().getSimpleName();

    hdfsBaseDir = conf.get(HDFS_BASE_DIR);
    Preconditions.checkState(StringUtils.isNotEmpty(hdfsBaseDir),
        name + " " + HDFS_BASE_DIR + " must be specified");
    LOGGER.info("Property: " + HDFS_BASE_DIR + "=" + hdfsBaseDir);
  }

  @Override
  public void start() {
    LOGGER.info("{} started", name);
  }

  @Override
  public void stop() {
    LOGGER.info("{} stopped", name);
  }
}
