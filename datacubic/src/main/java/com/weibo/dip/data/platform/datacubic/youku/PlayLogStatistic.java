package com.weibo.dip.data.platform.datacubic.youku;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.datacubic.util.LoggerUtil;
import com.weibo.dip.data.platform.datacubic.youku.entity.PlayLog;
import com.weibo.dip.data.platform.datacubic.youku.entity.Result;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetDomainId;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetHour;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetPlatform;
import com.weibo.dip.data.platform.datacubic.youku.udf.GetVideoId;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;

/**
 * @author delia
 */

public class PlayLogStatistic {

  private static final Logger LOGGER = LoggerUtil.getLogger(PlayLogStatistic.class);

  private static final String HDFS_INPUT = "/user/hdfs/rawlog/app_weibomobilekafka1234_weibomobileaction799";

  private static final String HDFS_OUTPUT = "/tmp/youku";

  private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

  private static final SimpleDateFormat YYYY_MM_DD = new SimpleDateFormat("yyyy_MM_dd");

  private static final SimpleDateFormat YMD = new SimpleDateFormat("yyyyMMdd");

  private static String getYesterday(Date today, SimpleDateFormat format) {
    Calendar cal = Calendar.getInstance();

    cal.setTime(today);

    cal.add(Calendar.DAY_OF_MONTH, -1);

    return format.format(cal.getTime());
  }

  private static String getInputPath(Date today) {
    return HDFS_INPUT + "/" + getYesterday(today, YYYY_MM_DD) + "/*";
  }

  private static String getOutputPath(Date today) {
    return HDFS_OUTPUT + "/" + getYesterday(today, YYYY_MM_DD);
  }

  private static Date StrToDate(String time) throws ParseException {
    return YMD.parse(time);
  }

  private static String getSQL() throws Exception {
    return String.join("\n", IOUtils
        .readLines(PlayLogStatistic.class.getClassLoader().getResourceAsStream("youku_play.sql"),
            CharEncoding.UTF_8));
  }

  private static void writeOut(Date today, List<Row> rows) throws Exception {
    int size = CollectionUtils.isEmpty(rows) ? 0 : rows.size();

    LOGGER.info("size: " + size);

    Map<String, Map<String, Result>> domainids = new HashMap<>();

    for (Row row : rows) {
      String videoid = row.getString(0);
      String domainid = row.getString(1);
      String platform = row.getString(2);
      int hour = Integer.parseInt(row.getString(3));
      long count = row.getLong(4);

      if (!domainids.containsKey(domainid)) {
        domainids.put(domainid, new HashMap<>());
      }

      Map<String, Result> videoids = domainids.get(domainid);

      if (!videoids.containsKey(videoid)) {
        videoids.put(videoid, new Result(videoid, domainid));
      }

      videoids.get(videoid).setData(platform, hour, count);
    }

    Path outputPath = new Path(getOutputPath(today));

    FileSystem fs = FileSystem.get(new Configuration());

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);

      LOGGER.info("outputPath " + outputPath + " already exists, deleted");
    }

    for (Map.Entry<String, Map<String, Result>> domainidEntry : domainids.entrySet()) {
      String domainid = domainidEntry.getKey();
      Map<String, Result> videoids = domainidEntry.getValue();
      String fileName = "domainid_" + domainid + ".txt";

      Path domainidOutputPath = new Path(outputPath, fileName);

      LOGGER.info("write data to " + domainidOutputPath);

      BufferedWriter writer = null;

      try {
        writer = new BufferedWriter(
            new OutputStreamWriter(fs.create(domainidOutputPath), CharEncoding.UTF_8));

        for (Map.Entry<String, Result> videoEntry : videoids.entrySet()) {
          writer.write(GsonUtil.toJson(videoEntry.getValue()));
          writer.newLine();
        }
      } finally {
        if (writer != null) {
          writer.close();
        }
      }
    }
  }

  public static void main(String[] args) {
    Date today;
    SparkSession sparkSession = null;
    JavaSparkContext sparkContext = null;
    //AlarmService alarmService = ServiceProxyBuilder.buildLocalhost(AlarmService.class);

    try {
      sparkSession = SparkSession.builder().getOrCreate();
      sparkContext = new JavaSparkContext(sparkSession.sparkContext());

      if (ArrayUtils.isEmpty(args)) {
        today = new Date();

      } else {
        if (args.length != 1) {
          LOGGER.error("input time param len = 1!");
          return;
        }
        String time = args[0];
        Pattern pattern = Pattern.compile("^\\d{8}$");
        Matcher matcher = pattern.matcher(time);
        if (!matcher.matches()) {
          LOGGER.error("input time digital like YYYYMMDD!");
          return;
        }

        today = StrToDate(time);
      }

      LOGGER.info("inputPath: " + getInputPath(today));

      JavaRDD<String> srcRdd = sparkContext.textFile(getInputPath(today));

      JavaRDD<PlayLog> desDataset = srcRdd.map(PlayLog::build).filter(Objects::nonNull);

      sparkSession.createDataFrame(desDataset, PlayLog.class).createOrReplaceTempView("logdata");

      sparkSession.udf().register("getVideoId", new GetVideoId(), DataTypes.StringType);
      sparkSession.udf().register("getDomainId", new GetDomainId(), DataTypes.StringType);
      sparkSession.udf().register("getPlatform", new GetPlatform(), DataTypes.StringType);
      sparkSession.udf()
          .register("getHour", new GetHour(getYesterday(today, YYYYMMDD)), DataTypes.StringType);

      String sql = getSQL();

      LOGGER.info("sql: " + sql);

      Dataset<Row> results = sparkSession.sql(sql);

      List<Row> rows = results.javaRDD().collect();

      writeOut(today, rows);
    } catch (Exception e) {
      LOGGER.error("Execute error: " + ExceptionUtils.getFullStackTrace(e));
      //alarmService.sendAlarmToGroups("DataCubic", "PlayLogStatistic", e.getMessage(), ExceptionUtils.getFullStackTrace(e), new String[]{"DIP_ALL"});
    } finally {
      if (sparkContext != null) {
        sparkContext.close();
      }

      if (sparkSession != null) {
        sparkSession.stop();
      }
    }
  }

}



