package com.weibo.dip.data.platform.falcon.scheduler.util;

import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Wen on 2017/2/16.
 *
 */
public class HdfsUtils {

    private static final HdfsService HDFS_SERVICE ;

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

    static {
        HDFS_SERVICE = ServiceProxyBuilder.buildLocalhost(HdfsService.class);
    }

    private HdfsUtils(){

    }

    public static HdfsService getInstance(){
        return HDFS_SERVICE;
    }

    public static List<String> pathFormat(String path,String category,long startTime,long endTime) {
        List<String> hourlyDirList = new ArrayList<>(); //近几个小时的目录
        int range = ((int) (endTime - startTime) / 1000 / 60 / 60) + 1; //取至少startTime开始三个小时的文件
        Date date = new Date(startTime - 60 * 60 * 1000);              //从startTime上个小时开始
        for (int i = 0; i < range + 1; i++) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MINUTE, i * 60);                            //startTime后的第i个小时——endTime后两小时
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int day = cal.get(Calendar.DAY_OF_MONTH);
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            String hourlyDir = path + "/" + category + "/" + year + "_" + (Integer.toString(month).length() == 2 ? month : "0" + month) + "_" + (Integer.toString(day).length() == 2 ? day : "0" + day) + "/" + (Integer.toString(hour).length() == 2 ? hour : "0" + hour);
            hourlyDirList.add(hourlyDir);
            if (i == range) {
                Calendar endCalendar = Calendar.getInstance();
                endCalendar.setTime(new Date(endTime));
                if (hour != endCalendar.get(Calendar.HOUR_OF_DAY)) {
                    year = endCalendar.get(Calendar.YEAR);
                    month = endCalendar.get(Calendar.MONTH) + 1;
                    day = endCalendar.get(Calendar.DAY_OF_MONTH);
                    hour = endCalendar.get(Calendar.HOUR_OF_DAY);
                    hourlyDir = path + "/" + category + "/" + year + "_" + (Integer.toString(month).length() == 2 ? month : "0" + month) + "_" + (Integer.toString(day).length() == 2 ? day : "0" + day) + "/" + (Integer.toString(hour).length() == 2 ? hour : "0" + hour);
                    hourlyDirList.add(hourlyDir);
                }
            }
        }
        return hourlyDirList;
    }
    public static List<HFileStatus> getRecentFileListByHdfs(String category,long startTime,long endTime) {
        List<HFileStatus> hFileStatusList = new ArrayList<>();
        long cost = new Date().getTime();
        String path = "/user/hdfs/rawlog";
        List<String> hourlyDirList = pathFormat(path,category, startTime, endTime);

        for (String hourlyDir : hourlyDirList) {
            List<HFileStatus> hourlyFilesList = null;
            try {
                hourlyFilesList = HDFS_SERVICE.listFiles(hourlyDir);
            } catch (Exception e) {
                LOGGER.error(ExceptionUtils.getFullStackTrace(e));
            }
            if (!CollectionUtils.isEmpty(hourlyDirList)) {
                if (hourlyFilesList != null) {
                    hFileStatusList.addAll(hourlyFilesList.stream().filter(hFileStatus -> hFileStatus.getModificationTime()>startTime && hFileStatus.getModificationTime() <= endTime).collect(Collectors.toList()));
                }
            }
        }

        LOGGER.info("Finished getRecentFileListByHdfs .Time escapes " + Long.toString(new Date().getTime() - cost) + "ms");

        return hFileStatusList;
    }
}
