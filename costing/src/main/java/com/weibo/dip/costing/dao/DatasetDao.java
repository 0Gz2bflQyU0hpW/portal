package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.exception.HDFSAccessException;
import com.weibo.dip.costing.bean.Dataset;
import java.io.IOException;
import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/19.
 */
public interface DatasetDao {

  int add(Dataset dataset) throws DataAccessException;

  int delete(int id) throws DataAccessException;

  int delete(String name) throws DataAccessException, HDFSAccessException;

  int deleteFromDB(String name) throws DataAccessException;

  void deleteFromHDFS(String name) throws HDFSAccessException;

  Dataset get(int id) throws DataAccessException;

  Dataset get(String name) throws DataAccessException;

  boolean exist(String name) throws DataAccessException;

  List<Dataset> loadFromDB() throws DataAccessException;

  List<String> loadFromHDFS() throws HDFSAccessException;

  long size() throws IOException;

  long size(String name) throws HDFSAccessException;

}
