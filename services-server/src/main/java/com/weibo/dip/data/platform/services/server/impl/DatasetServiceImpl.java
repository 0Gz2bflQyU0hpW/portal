package com.weibo.dip.data.platform.services.server.impl;

import com.weib.dip.data.platform.services.client.DatasetService;
import com.weib.dip.data.platform.services.client.model.Dataset;
import com.weibo.dip.data.platform.services.server.dao.DatasetDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by ftx on 2016/12/28.
 */
@Service
public class DatasetServiceImpl implements DatasetService{

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetServiceImpl.class);
    @Autowired
    private DatasetDao datasetDao;


    @Override
    public List<Dataset> getDatasetList() throws Exception {
        return datasetDao.datasetList();
    }

    @Override
    public List<String> getDatasetNameList() throws Exception {
        return datasetDao.datasetNameList();
    }
}
