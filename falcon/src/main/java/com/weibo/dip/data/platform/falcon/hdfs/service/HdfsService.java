package com.weibo.dip.data.platform.falcon.hdfs.service;

import com.weib.dip.data.platform.services.client.DatasetService;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import com.weibo.dip.data.platform.falcon.hdfs.model.LogDailySize;
import com.weibo.dip.data.platform.falcon.hdfs.model.LogHourlySize;
import com.weibo.dip.data.platform.falcon.hdfs.model.LogInfo;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by Wen on 2016/12/22.
 *
 */

public class HdfsService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsService.class);

    public Map<String, LogInfo> getLogSizeByHourly() throws Exception {
        com.weib.dip.data.platform.services.client.HdfsService hdfsService = ServiceProxyBuilder.buildLocalhost(com.weib.dip.data.platform.services.client.HdfsService.class);
        DatasetService datasetService = ServiceProxyBuilder.buildLocalhost(DatasetService.class);
        List<String> datasetName = datasetService.getDatasetNameList();
//        System.out.println(datasetName);
        String rawlog = "/user/hdfs/rawlog";//  rawlog/
        List<HFileStatus> logsFileStatus = hdfsService.listDirs(rawlog);
        Map<String, LogInfo> logsMap = new HashMap<>();

        for (HFileStatus logFileStatus : logsFileStatus) {
            if (!datasetName.contains(logFileStatus.getName())) {
                continue;
            }
            if (Character.isDigit(logFileStatus.getName().charAt(0))) {
                System.err.println("the format is wrong!!" + "current path: " + logFileStatus.getPath());
                continue;
            }
            LogInfo log = new LogInfo();
            Map<String, LogDailySize> logDailyMap = new HashMap<>();
            List<HFileStatus> logDaily = hdfsService.listDirs(logFileStatus.getPath());
            if (CollectionUtils.isEmpty(logDaily))
                continue;
            for (HFileStatus logDailyFileStatus : logDaily) {
                if (!logDailyFileStatus.getName().matches("20[0-9]{2}_[0-1][0-9]_[0-3][0-9]")) {
                    System.err.println("the format is wrong!!" + "current path: " + logDailyFileStatus.getPath());
                    continue;
                }
                LogDailySize logDailySize = new LogDailySize();
                Map<String, LogHourlySize> logHourlyMap = new HashMap<>();
                List<HFileStatus> logHourly = hdfsService.listDirs(logDailyFileStatus.getPath());
                if (CollectionUtils.isEmpty(logHourly))
                    continue;
                for (HFileStatus logHourlyFileStatus : logHourly) {
                    if (!logHourlyFileStatus.getName().matches("[0-2][0-9]")) {
                        System.err.println("the format is wrong!!" + "current path: " + logHourlyFileStatus.getPath());
                        continue;
                    }
                    LogHourlySize logHourlySize = new LogHourlySize();
                    logHourlySize.setHour(logHourlyFileStatus.getName());
                    logHourlySize.setSize(hdfsService.getLength(logHourlyFileStatus.getPath()));
//							ft.getModificationTime();  //true false策略
                    logHourlyMap.put(logHourlyFileStatus.getName(), logHourlySize);
                }

                logDailySize.setDate(logDailyFileStatus.getName());
                logDailySize.setLogSizeMap(logHourlyMap);
                logDailyMap.put(logDailySize.getDate(), logDailySize);

                log.setLogName(logFileStatus.getName());
                log.setDailyMap(logDailyMap);
                logsMap.put(log.getLogName(), log);
            }
        }
        return logsMap;
    }

    /**
     * @param path      /user/hdfs/rawlog
     * @param category  logNane
     * @param startTime :ms
     * @param endTime   :ms
     * @return List<String>
     */
    private List<String> getReccentDirPath(String path, String category, long startTime, long endTime) throws Exception {
//        long timeOutEscape = 600000;
//        long bandwidth = 1000 * (int)Math.pow(2,20);
//        long speed = 1000*bandwidth/8;  //ms

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
//            StringBuffer sb = new StringBuffer();
//            sb.append(path).append("/").append(category).append("/").append(year).append("_").append(Integer.toString(month).length()==2?month:"0"+month).append("_").append(Integer.toString(day).length()==2?day:"0"+day).append("/").append(Integer.toString(hour).length()==2?hour:"0"+hour).append("/");
//            hourlyDirList.add(sb.toString());
////            String year = Integer.toString(cal.get(Calendar.YEAR));
////            String month = Integer.toString(cal.get(Calendar.MONTH)+1).length()==2?Integer.toString( cal.get(Calendar.MONTH)+1):"0"+Integer.toString( cal.get(Calendar.MONTH)+1);
////            String day = Integer.toString(cal.get(Calendar.DAY_OF_MONTH)).length()==2?Integer.toString( cal.get(Calendar.DAY_OF_MONTH)):"0"+Integer.toString( cal.get(Calendar.DAY_OF_MONTH));
////            String hour = Integer.toString(cal.get(Calendar.HOUR_OF_DAY)).length()==2?Integer.toString( cal.get(Calendar.HOUR_OF_DAY)):"0"+Integer.toString( cal.get(Calendar.HOUR_OF_DAY));
////            String hourlyDir = path + "/" + category + "/" + year + "_" + month + "_" + day + "/" + hour;
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

    /**
     * @param path      path
     * @param category  数据集名称
     * @param startTime 开始时间（默认为当前时间-strategy）
     * @param endTime finishTime
     * @return List<HFileStatus>
     */
//    public List<HFileStatus> getRecentFinishedFiles(String path, String category, long startTime, long strategy) throws Exception {
//        com.weib.dip.data.platform.services.client.HdfsService hdfsService = ServiceProxyBuilder.buildLocalhost(com.weib.dip.data.platform.services.client.HdfsService.class);
//
//        List<HFileStatus> hFileStatusList = new ArrayList<>();
//        List<HFileStatus> finishedHFileStatusList = new ArrayList<>();
//        int count = 0;           //执行strategy的次数
//        while (true) {
//
//            long cost = new Date().getTime();
//            LOGGER.info("Start......Current startTime is " + startTime);
//
//            List<String> hourlyDirList = getReccentDirPath(path, category, startTime, new Date().getTime());
//
//            startTime = new Date().getTime();
//
//            Map<String, Long> fileModifyTimeMap = new HashMap<>();
//            for (String hourlyDir : hourlyDirList) {
//                List<HFileStatus> hourlyFilesList = hdfsService.listFiles(hourlyDir);
//                for (HFileStatus hFileStatus : hourlyFilesList) {
//                    fileModifyTimeMap.put(hFileStatus.getPath(), hFileStatus.getModificationTime());
//                }
//            }
//
//            Thread.sleep(strategy);  //strategy 时间过后再看modifyTime是否改变
//
//            for (String hourlyDir : hourlyDirList) {
//                List<HFileStatus> hourlyFilesList = hdfsService.listFiles(hourlyDir);
//                if (!CollectionUtils.isEmpty(hourlyFilesList))
//                    hFileStatusList.addAll(hourlyFilesList.stream().filter(hFileStatus -> fileModifyTimeMap.containsKey(hFileStatus.getPath())).filter(hFileStatus -> fileModifyTimeMap.get(hFileStatus.getPath()) == hFileStatus.getModificationTime()).collect(Collectors.toList()));
//
//            }
//            if (!CollectionUtils.isEmpty(finishedHFileStatusList))
//                hFileStatusList.removeAll(finishedHFileStatusList);
//
//            finishedHFileStatusList.addAll(hFileStatusList);
//
//            LOGGER.info("Time escapes " + Long.toString(new Date().getTime() - cost) + "ms");
//            count++;
//        }
//    }
    public List<HFileStatus> getRecentFinishedFiles(String path, String category, long startTime, long endTime) throws Exception {
        com.weib.dip.data.platform.services.client.HdfsService hdfsService = ServiceProxyBuilder.buildLocalhost(com.weib.dip.data.platform.services.client.HdfsService.class);
        List<HFileStatus> hFileStatusList = new ArrayList<>();
        long cost = new Date().getTime();
        LOGGER.info("Start......Current startTime is " + startTime);

        List<String> hourlyDirList = getReccentDirPath(path, category, startTime, endTime);

        for (String hourlyDir : hourlyDirList) {
            List<HFileStatus> hourlyFilesList = hdfsService.listFiles(hourlyDir);
            if (!CollectionUtils.isEmpty(hourlyDirList)) {
                hFileStatusList.addAll(hourlyFilesList.stream().filter(hFileStatus -> hFileStatus.getModificationTime()>=startTime && hFileStatus.getModificationTime() < endTime).collect(Collectors.toList()));
            }
        }

        LOGGER.info("Time escapes " + Long.toString(new Date().getTime() - cost) + "ms");

        return hFileStatusList;
    }
//        while (true) {
//            List<HFileStatus> hFileStatusList = new ArrayList<>();
//            long cost = new Date().getTime();
//            LOGGER.info("Start......Current startTime is " + startTime);
//
//            long endTime = new Date().getTime();
//
//            List<String> hourlyDirList = getReccentDirPath(path, category, startTime, endTime);
//
//            startTime = endTime;
//
//            Map<String, Long> fileModifyTimeMap = new HashMap<>();
//            for (String hourlyDir : hourlyDirList) {
//                List<HFileStatus> hourlyFilesList = hdfsService.listFiles(hourlyDir);
//                if (!CollectionUtils.isEmpty(hourlyDirList)){
//                    for (HFileStatus hFileStatus : hourlyFilesList) {
//                        if (hFileStatus.getModificationTime()<=startTime+strategy&&hFileStatus.getModificationTime()>=startTime)
//                            fileModifyTimeMap.put(hFileStatus.getPath(), hFileStatus.getModificationTime());
//                    }
//                }
//            }
//
//            Thread.sleep(strategy);  //strategy 时间过后再看modifyTime是否改变
//
//            for (String hourlyDir : hourlyDirList) {
//                List<HFileStatus> hourlyFilesList = hdfsService.listFiles(hourlyDir);
////                if (!CollectionUtils.isEmpty(hourlyFilesList))
////                    hFileStatusList.addAll(hourlyFilesList.stream().filter(hFileStatus -> fileModifyTimeMap.containsKey(hFileStatus.getPath())).filter(hFileStatus -> fileModifyTimeMap.get(hFileStatus.getPath()) == hFileStatus.getModificationTime()).collect(Collectors.toList()));
//                if (!CollectionUtils.isEmpty(hourlyFilesList)){
//                    for (HFileStatus hFileStauts : hourlyFilesList){
//                        if (hFileStauts.getModificationTime()>=startTime&&hFileStauts.getModificationTime()<=startTime+strategy){
//                            if (fileModifyTimeMap.containsKey(hFileStauts.getPath())){
//                                if (fileModifyTimeMap.get(hFileStauts.getPath())==hFileStauts.getModificationTime())
//                                    hFileStatusList.add(hFileStauts);
//                            }
//                        }
//                    }
//                }
//            }
//
//            LOGGER.info("Time escapes " + Long.toString(new Date().getTime() - cost) + "ms");
//        }
}
