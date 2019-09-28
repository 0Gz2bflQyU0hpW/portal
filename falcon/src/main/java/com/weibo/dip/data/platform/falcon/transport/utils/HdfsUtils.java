package com.weibo.dip.data.platform.falcon.transport.utils;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;

/**
 * Created by Wen on 2017/2/11.

 */
public class HdfsUtils {

    private static final FileSystem FILE_SYSTEM;

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

    static {
        Configuration configuration = new Configuration();
        try {
            configuration.set("fs.defaultFS", "hdfs://192.168.136.150:9000");
            FILE_SYSTEM = FileSystem.get(configuration);
        } catch (IOException e) {
            LOGGER.error("Init filesystem...failed" + ExceptionUtils.getFullStackTrace(e));
            throw new ExceptionInInitializerError(e);
        }
    }

    public static FileSystem getFileSystem() {
        return FILE_SYSTEM;
    }

    public static FileSystem accessByHdfs() throws Exception {
        Class<?> clazz = Class.forName("org.apache.hadoop.security.User");
        Constructor<?> ctor = clazz.getConstructor(String.class);
        ctor.setAccessible(true);

        Principal principal = (Principal) ctor.newInstance("hdfs");
        Subject subject = new Subject(false, Collections.singleton(principal),Collections.emptySet(),Collections.emptySet());
        return Subject.doAs(subject, new PrivilegedAction<FileSystem>() {
            @Override
            public FileSystem run() {
                FileSystem fileSystem = null;
                try {
                    fileSystem = FileSystem.get(new URI("hdfs://testhadoop"),new Configuration());
                } catch (IOException | URISyntaxException e) {
                    e.printStackTrace();
                }
                return  fileSystem;
            }
        });
    }

    private HdfsUtils() {

    }

    public static void main(String[] args) {
        System.out.println(FILE_SYSTEM);
    }
}
