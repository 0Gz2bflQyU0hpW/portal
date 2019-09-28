package com.weibo.dip.data.platform.services.server.dao;

import com.weib.dip.data.platform.services.client.model.Dataset;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ftx on 2016/12/28.
 */
public interface DatasetDao {

    List<Dataset> datasetList() throws SQLException;

    List<String>  datasetNameList()throws SQLException;
}
