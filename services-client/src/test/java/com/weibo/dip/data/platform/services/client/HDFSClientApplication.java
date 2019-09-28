package com.weibo.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.DatasetService;
import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.YarnService;
import com.weib.dip.data.platform.services.client.model.Dataset;
import com.weib.dip.data.platform.services.client.model.HApplicationId;
import com.weib.dip.data.platform.services.client.model.HApplicationReport;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weib.dip.data.platform.services.client.util.ServiceProxyBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by yurun on 16/11/28.
 */
public class HDFSClientApplication {

    public static void main(String[] args) throws Exception {
        HdfsService hdfsService = ServiceProxyBuilder.buildLocalhost(HdfsService.class);
        YarnService yarnService = ServiceProxyBuilder.buildLocalhost(YarnService.class);
        DatasetService datasetService=ServiceProxyBuilder.buildLocalhost(DatasetService.class);

//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
        String path = "/user/hdfs/rawlog/test";

        while(true) {
            List<HFileStatus> hFileStatusList = hdfsService.listDirs(path,false);
            for (HFileStatus hFileStatus : hFileStatusList) {
                System.out.println(hFileStatus.getBlockSize());
                System.out.println("Name:" + hFileStatus.getName() + " Modify Time:" + hFileStatus.getModificationTime() + " Size:"+hdfsService.getLength(hFileStatus.getPath()));
            }
            Thread.sleep(10);
        }

//        System.out.println(hdfsService.exist("/user/yurun"));
//        System.out.println(hdfsService.listFiles("/user/hdfs/result", true));
//        EnumSet<YarnApplicationState> yarnApplicationStateEnumSet = EnumSet.of(YarnApplicationState.RUNNING);
//        List<HApplicationReport> applications = yarnService.getApplications(yarnApplicationStateEnumSet);
//        for (HApplicationReport application : applications) {
//            System.out.println(application.toString());
//        }

//        HApplicationId appId=new HApplicationId(4958,1482289406403L);
//        yarnService.killApplication(appId);

//        List<Dataset> datasetList = datasetService.getDatasetList();
//        System.out.println(datasetList);
//        List<String> datasetNameList = datasetService.getDatasetNameList();
//        System.out.println(datasetNameList);
    }


}
