package org.apache.log4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.log4j.helpers.LogLog;

/**
 * Enhanced DailyRollingFileAppender: support maxBackupIndex.
 *
 * @author yurun
 */
public class EnhancedDailyRollingFileAppender extends DailyRollingFileAppender {
  protected int maxBackupIndex = 1;

  public void setMaxBackupIndex(int maxBackups) {
    this.maxBackupIndex = maxBackups;
  }

  @Override
  public boolean isAsSevereAsThreshold(Priority priority) {
    return (threshold == null) || threshold.equals(priority);
  }

  @Override
  void rollOver() throws IOException {
    super.rollOver();

    File logFile = new File(fileName);

    if (!logFile.exists()) {
      LogLog.error(fileName + " not exist");

      return;
    }

    File logDir = logFile.getParentFile();

    File[] logFiles =
        logDir.listFiles(
            (dir, filename) ->
                !filename.equals(logFile.getName()) && filename.startsWith(logFile.getName()));

    if (logFiles == null || logFiles.length == 0) {
      return;
    }

    Arrays.sort(logFiles, Comparator.comparing(File::getName));

    for (int index = 0; index < logFiles.length - maxBackupIndex; index++) {
      logFiles[index].delete();
    }
  }
}
