package com.weibo.dip.costing.service.impl;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.weibo.dip.costing.bean.Product;
import com.weibo.dip.costing.dao.ProductDao;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Created by yurun on 18/4/18.
 */
@Service
public class ProductServiceImpl implements ProductService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProductServiceImpl.class);

  private static final String RDDSS_API = "rddss.api";

  @Autowired
  private ProductDao productDao;

  @Autowired
  private Environment env;

  @Override
  public void sync() throws Exception {
    String response = HttpClientUtil.doGet(env.getProperty(RDDSS_API));

    JsonArray array = GsonUtil.parse(response).getAsJsonArray();

    for (JsonElement element : array) {
      JsonObject object = element.getAsJsonObject();

      JsonElement uuid = object.get("uuid");
      JsonElement name = object.get("name");
      JsonElement techLeader = object.get("tech_leader");
      JsonElement productLeader = object.get("product_leader");
      JsonElement vicePresident = object.get("vice_president");
      JsonElement status = object.get("status");
      JsonElement isPlatform = object.get("is_platform");

      Product product = new Product();

      product.setUuid(object.get("uuid").getAsString());
      product.setName(object.get("name").getAsString());
      product.setTechLeader(!techLeader.isJsonNull() ? techLeader.getAsString() : "");
      product.setProductLeader(!productLeader.isJsonNull() ? productLeader.getAsString() : "");
      product.setVicePresident(!vicePresident.isJsonNull() ? vicePresident.getAsString() : "");
      product.setStatus(!status.isJsonNull() ? status.getAsString() : "0");
      product.setPlatform(!isPlatform.isJsonNull() && isPlatform.getAsString().equals("1"));

      if (productDao.exist(product.getUuid())) {
        productDao.update(product);
      } else {
        productDao.add(product);
      }
    }

    LOGGER.info("product sync success");
  }

  @Override
  public List<Product> gets() throws DataAccessException {
    return productDao.gets();
  }

  @Override
  public Product getByUuid(String uuid) throws DataAccessException {
    return productDao.getByUuid(uuid);
  }

  @Override
  public boolean existByUuid(String uuid) throws DataAccessException {
    return Objects.nonNull(getByUuid(uuid));
  }

  @Override
  public boolean existByName(String name) throws DataAccessException {
    return Objects.nonNull(productDao.getByName(name));
  }

}
