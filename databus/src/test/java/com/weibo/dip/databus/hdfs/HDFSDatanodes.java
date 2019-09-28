package com.weibo.dip.databus.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;

/**
 * Created by yurun on 17/12/7.
 */
public class HDFSDatanodes {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);

        DatanodeInfo[] datanodes = fs.getDataNodeStats();

        for (DatanodeInfo datanode : datanodes) {
            System.out.println(datanode.getHostName());
        }

        fs.close();
    }

}
