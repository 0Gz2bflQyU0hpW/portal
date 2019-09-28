package com.weib.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.model.HFileStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;

/**
 * Created by yurun on 16/12/14.
 */
public interface HdfsService {

    boolean exist(String path) throws Exception;

    boolean isFile(String path) throws Exception;

    boolean isDir(String path) throws Exception;

    boolean delete(String path)throws Exception;

    List<HFileStatus> listStatus(String path) throws Exception;

    List<HFileStatus> listStatus(String path, boolean recursive) throws Exception;

    List<HFileStatus> listFiles(String path) throws Exception;

    List<HFileStatus> listFiles(String path, boolean recursive) throws Exception;

    List<HFileStatus> listDirs(String path) throws Exception;

    List<HFileStatus> listDirs(String path, boolean recursive) throws Exception;

    ContentSummary getContentSummary(String path) throws Exception;

    long getLength(String path) throws Exception;


}
