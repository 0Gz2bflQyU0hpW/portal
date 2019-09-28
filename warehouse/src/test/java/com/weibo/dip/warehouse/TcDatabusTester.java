package com.weibo.dip.warehouse;

import com.weibo.dip.data.platform.commons.util.HDFSUtil;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.hadoop.fs.ContentSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author yurun */
public class TcDatabusTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(TcDatabusTester.class);

  public static void main(String[] args) throws Exception {
    List<String> datasets;

    InputStream in = null;

    try {
      in = TcDatabusTester.class.getClassLoader().getResourceAsStream("datasets");

      datasets = IOUtils.readLines(in, CharEncoding.UTF_8);
    } finally {
      if (Objects.nonNull(in)) {
        in.close();
      }
    }

    for (String dataset : datasets) {
      ContentSummary summary = HDFSUtil.summary("/user/hdfs/rawlog/" + dataset);
      if (Objects.isNull(summary) || summary.getLength() <= 0) {
        LOGGER.info(dataset);
      }
    }
  }
}
