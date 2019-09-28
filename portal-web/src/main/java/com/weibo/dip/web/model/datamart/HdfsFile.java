package com.weibo.dip.web.model.datamart;

import java.util.Date;
import org.apache.hadoop.fs.Path;

/**
 * Created by shixi_dongxue3 on 2017/12/14.
 */
public class HdfsFile {

  //路径
  private Path hdfsFilePath;

  //文件名
  private String hdfsFileName;

  //大小
  private float hdfsFilesize;

  //文件内容
  private StringBuffer hdfsFileContent;

  //文件修改时间
  private Date createTime;

  public HdfsFile() {
  }

  /*getters and setters*/
  public String getHdfsFileName() {
    return hdfsFileName;
  }

  public void setHdfsFileName(String hdfsFileName) {
    this.hdfsFileName = hdfsFileName;
  }

  public float getHdfsFilesize() {
    return hdfsFilesize;
  }

  public void setHdfsFilesize(float hdfsFilesize) {
    this.hdfsFilesize = hdfsFilesize;
  }

  public Path getHdfsFilePath() {
    return hdfsFilePath;
  }

  public void setHdfsFilePath(Path hdfsFilePath) {
    this.hdfsFilePath = hdfsFilePath;
  }

  public StringBuffer getHdfsFileContent() {
    return hdfsFileContent;
  }

  public void setHdfsFileContent(StringBuffer hdfsFileContent) {
    this.hdfsFileContent = hdfsFileContent;
  }

}
