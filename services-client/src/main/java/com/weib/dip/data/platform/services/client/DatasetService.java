package com.weib.dip.data.platform.services.client;

import com.weib.dip.data.platform.services.client.model.Dataset;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by ftx on 2016/12/28.
 */
public interface DatasetService {

    public List<Dataset> getDatasetList() throws Exception;

    public List<String> getDatasetNameList() throws Exception;
}
