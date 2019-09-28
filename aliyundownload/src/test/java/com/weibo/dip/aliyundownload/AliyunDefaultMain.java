package com.weibo.dip.aliyundownload;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import java.util.List;
import org.apache.hadoop.fs.Path;

/** @author yurun */
public class AliyunDefaultMain {

  public static void main(String[] args) throws Exception {
    String hdfsPath = "/tmp/aliyunXweibo/";

    List<Path> files = HDFSUtil.listFiles(hdfsPath, true);

    long size = 0;

    for (Path file : files) {
      if (!file.getName().contains("www_spoollxrsaansnq8tjw0_aliyunXweibo")) {
        size += HDFSUtil.getFileSize(file);
      }
    }

    System.out.println(size);
  }
}
