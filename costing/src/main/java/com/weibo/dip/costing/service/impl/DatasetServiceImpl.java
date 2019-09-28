package com.weibo.dip.costing.service.impl;

import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.dao.DatasetDao;
import com.weibo.dip.costing.exception.HDFSAccessException;
import com.weibo.dip.costing.service.ConsoleService;
import com.weibo.dip.costing.service.DatasetService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by yurun on 18/4/19.
 */
@Service
public class DatasetServiceImpl implements DatasetService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetServiceImpl.class);

  @Autowired
  private ConsoleService consoleService;

  @Autowired
  private DatasetDao datasetDao;

  @Override
  @Transactional(rollbackFor = DataAccessException.class)
  public int add(Dataset dataset) throws DataAccessException {
    return datasetDao.add(dataset);
  }

  @Override
  public void sync(boolean delete) throws DataAccessException, HDFSAccessException {
    List<Dataset> consoleDatasets = consoleService.gets();

    for (Dataset consoleDataset : consoleDatasets) {
      if (!exist(consoleDataset.getName())) {
        add(consoleDataset);
      }
    }

    List<Dataset> datasetsInDB = datasetDao.loadFromDB();
    List<String> datasetsInHDFS = datasetDao.loadFromHDFS();

    if (CollectionUtils.isNotEmpty(datasetsInHDFS)) {
      Set<String> datasets = Objects.nonNull(datasetsInDB)
          ? datasetsInDB.stream().map(Dataset::getName).collect(Collectors.toSet())
          : Collections.emptySet();

      datasetsInHDFS
          .stream()
          .filter(dataset -> !datasets.contains(dataset))
          .forEach(dataset -> {
            if (delete) {
              datasetDao.deleteFromHDFS(dataset);

              LOGGER.warn("dataset {} exists in hdfs, but no db record, delete from hdfs", dataset);
            } else {
              LOGGER.warn("dataset {} exists in hdfs, but no db record", dataset);
            }
          });

      datasetsInHDFS
          .stream()
          .filter(datasets::contains)
          .filter(dataset -> size(dataset) == 0)
          .forEach(dataset -> {
            if (delete) {
              datasetDao.deleteFromHDFS(dataset);

              LOGGER.warn("dataset {} exists in db, but size is 0, delete from hdfs", dataset);
            } else {
              LOGGER.warn("dataset {} exists in db, but size is 0", dataset);
            }
          });
    }
  }

  @Override
  public List<Dataset> gets() throws DataAccessException {
    return datasetDao.loadFromDB();
  }

  @Override
  public Dataset get(String name) throws DataAccessException {
    return datasetDao.get(name);
  }

  @Override
  public boolean exist(String name) throws DataAccessException {
    return datasetDao.exist(name);
  }

  @Override
  public long size() throws HDFSAccessException {
    try {
      return datasetDao.size();
    } catch (Exception e) {
      throw new HDFSAccessException(e.getMessage());
    }
  }

  @Override
  public long size(String category) throws HDFSAccessException {
    return datasetDao.size(category);
  }

  @Override
  public void delete(String category) throws IOException {
    datasetDao.delete(category);
  }

}
