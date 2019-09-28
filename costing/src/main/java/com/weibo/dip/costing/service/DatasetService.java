package com.weibo.dip.costing.service;

import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.exception.HDFSAccessException;
import java.io.IOException;
import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/19.
 */
public interface DatasetService {

  void sync(boolean delete) throws DataAccessException, HDFSAccessException;

  int add(Dataset dataset) throws DataAccessException;

  List<Dataset> gets() throws DataAccessException;

  Dataset get(String name) throws DataAccessException;

  boolean exist(String name) throws DataAccessException;

  long size() throws IOException;

  long size(String category) throws HDFSAccessException;

  void delete(String category) throws IOException;

}
