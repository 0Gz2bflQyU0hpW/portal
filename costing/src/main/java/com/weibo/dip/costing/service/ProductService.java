package com.weibo.dip.costing.service;

import com.weibo.dip.costing.bean.Product;
import java.util.List;
import org.springframework.dao.DataAccessException;

/**
 * Created by yurun on 18/4/18.
 */
public interface ProductService {

  void sync() throws Exception;

  List<Product> gets() throws DataAccessException;

  Product getByUuid(String uuid) throws DataAccessException;

  boolean existByUuid(String uuid) throws DataAccessException;

  boolean existByName(String name) throws DataAccessException;

}
