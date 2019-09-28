package com.weibo.dip.data.platform.falcon.hdfs.service.test;

import com.weibo.dip.data.platform.falcon.hdfs.model.LogInfo;
import com.weibo.dip.data.platform.falcon.hdfs.service.HdfsService;

import java.util.Map;

/**
 * Created by Wen on 2016/12/23.
 * hdfs Test
 */
public class TestHdfs {

    public static void main(String[] args) throws Exception {
        Map<String,LogInfo> map = new HdfsService().getLogSizeByHourly();
        System.out.println(map.size());
        System.out.println(map.keySet());
        for (String logName : map.keySet()){
            for (String logDate : map.get(logName).getDailyMap().keySet()) {
                for (String logHour : map.get(logName).getDailyMap().get(logDate).getLogSizeMap().keySet()){
                    System.out.println(logName+" "+logDate+" "+logHour+" logSize :"+ map.get(logName).getDailyMap().get(logDate).getLogSizeMap().get(logHour).getSize());
                }
            }
        }

    }
}
