package com.weibo.dip.data.platform.services.server.impl;

import com.google.common.base.Preconditions;
import com.weib.dip.data.platform.services.client.HdfsService;
import com.weib.dip.data.platform.services.client.model.HFileStatus;
import com.weibo.dip.data.platform.commons.util.HadoopConfiguration;
import org.apache.commons.collections.CollectionUtils;

import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by yurun on 16/12/14.
 *
 */
@Service
public class HdfsServiceImpl implements HdfsService {

    private static final FileSystem FILE_SYSTEM;
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsServiceImpl.class);
    static {
        try {
            FILE_SYSTEM = FileSystem.get(HadoopConfiguration.getInstance());
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public boolean exist(String path) throws Exception {
        return FILE_SYSTEM.exists(new Path(path));
    }

    private void checkPathExist(String path) throws Exception {
        Preconditions.checkState(exist(path), "Path " + path + " dose not exist");
    }

    @Override
    public boolean isFile(String path) throws Exception {
        return FILE_SYSTEM.isFile(new Path(path));
    }

    @Override
    public boolean isDir(String path) throws Exception {
        return FILE_SYSTEM.isDirectory(new Path(path));
    }

    @Override
    public boolean delete(String path) throws Exception {
        checkPathExist(path);
        if (isDir(path)) {
            LOGGER.info("Deleting dir "+path);
            return FILE_SYSTEM.delete(new Path(path), true);
        }
        else {
            LOGGER.info("Deleting file "+path);
            return FILE_SYSTEM.delete(new Path(path), false);
        }
    }

    @Override
    public List<HFileStatus> listStatus(String path) throws Exception {
        return listStatus(path, false);
    }

    @Override
    public List<HFileStatus> listStatus(String path, boolean recursive) throws Exception {
        checkPathExist(path);

        RemoteIterator<LocatedFileStatus> iterator = FILE_SYSTEM.listFiles(new Path(path), recursive);
        List<HFileStatus> hFileStatuses = new ArrayList<>();

        while (iterator.hasNext()) {
            hFileStatuses.add(new HFileStatus(iterator.next()));
        }

        return hFileStatuses;
    }

    @Override
    public List<HFileStatus> listFiles(String path) throws Exception {
        return listFiles(path, false);
    }

    @Override
    public List<HFileStatus> listFiles(String path, boolean recursive) throws Exception {
        List<HFileStatus> statuses = listStatus(path, recursive);

        if (CollectionUtils.isEmpty(statuses)) {
            return null;
        }

        return statuses.stream().filter(status -> !status.isdir()).collect(Collectors.toList());
    }

    @Override
    public List<HFileStatus> listDirs(String path) throws Exception {
        checkPathExist(path);
        List<FileStatus> fsList = Arrays.asList(FILE_SYSTEM.listStatus(new Path(path)));

        List<HFileStatus> hFileStatusList =  fsList.stream().map(HFileStatus::new).collect(Collectors.toList());

        if (CollectionUtils.isEmpty((hFileStatusList))){
            return new ArrayList<>();
        }

        return hFileStatusList.stream().filter(HFileStatus::isdir).collect(Collectors.toList());
    }

    @Override
    public List<HFileStatus> listDirs(String path, boolean recursive) throws Exception {
        if (!recursive){
            return listDirs(path);
        }
        List<HFileStatus> hFileStatusList = listDirs(path);

        List<HFileStatus> hFileStatusListRe = new ArrayList<>(hFileStatusList);

        for (HFileStatus hFileStatus : hFileStatusList){
            hFileStatusListRe.addAll(listDirs(hFileStatus.getPath(),true));
        }
        hFileStatusList = new ArrayList<>(hFileStatusListRe);

        return hFileStatusList;

    }

    @Override
    public ContentSummary getContentSummary(String path) throws Exception {
        checkPathExist(path);

        return FILE_SYSTEM.getContentSummary(new Path(path));
    }

    @Override
    public long getLength(String path) throws Exception {
        return getContentSummary(path).getLength();
    }

}
