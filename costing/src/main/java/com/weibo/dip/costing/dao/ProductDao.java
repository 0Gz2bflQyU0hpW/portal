package com.weibo.dip.costing.dao;

import com.weibo.dip.costing.bean.Product;
import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/18.
 */
public interface ProductDao {

  boolean exist(String uuid) throws DataAccessException;

  int add(Product product) throws DataAccessException;

  int delete(int id) throws DataAccessException;

  Product get(int id) throws DataAccessException;

  Product getByUuid(String uuid) throws DataAccessException;

  Product getByName(String name) throws DataAccessException;

  List<Product> gets() throws DataAccessException;

  int update(Product product) throws DataAccessException;

}
