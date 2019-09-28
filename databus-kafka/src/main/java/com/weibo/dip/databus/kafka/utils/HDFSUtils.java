package com.weibo.dip.databus.kafka.utils;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class HDFSUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSUtils.class);
    private HDFSUtils(){}
    private static FileSystem fileSystem;

    static{
        Configuration conf = new Configuration();
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            LOGGER.error("init FileSystem error \n{}", ExceptionUtils.getFullStackTrace(e));
            throw new ExceptionInInitializerError(e);
        }
    }

    public static BufferedReader getReader(String filePath) throws IOException {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(filePath))));
        } catch (IOException e) {
            throw e;
        }
        return bufferedReader;
    }

    public static BufferedWriter getWriter(String filePath) throws IOException {
        BufferedWriter bufferedWriter;
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(filePath))));
        } catch (IOException e) {
           throw e;
        }
        return bufferedWriter;
    }

    public static void close() throws IOException {
        fileSystem.close();
    }


    public static void main(String[] args) {
/*        try {
            long blockSize = fileSystem.getFileStatus(new Path("/user/jianhong1/rawlog/hot_nginx_access/2017_11_13/21/hot_nginx_access-bj-m-207618a.local-2017_11_13_21")).getLen();
            System.out.println(blockSize);
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        String filePath = "/user/jianhong1/rawlog/hot_nginx_access/2017_11_14/14/hot_nginx_access-bj-m-207618a.local-2017_11_14_14";
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = getReader(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String line;
        int recordNumber = 0;
        try {
            while((line = bufferedReader.readLine()) != null){
                System.out.println(line);
                recordNumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("record number: " + recordNumber);


    }

}
