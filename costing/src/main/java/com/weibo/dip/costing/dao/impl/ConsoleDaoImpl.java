package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.bean.Product;
import com.weibo.dip.costing.dao.ConsoleDao;
import com.weibo.dip.costing.dao.ProductDao;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/18.
 */
@Repository
public class ConsoleDaoImpl implements ConsoleDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleDaoImpl.class);

  @Autowired
  @Qualifier("consoleJdbcTemplate")
  private JdbcTemplate consoleJT;

  @Autowired
  private ProductDao productDao;

  private static class ConsoleDatasetRowMapper implements RowMapper<Dataset> {

    private ProductDao productDao;

    public ConsoleDatasetRowMapper(ProductDao productDao) {
      this.productDao = productDao;
    }

    @Override
    public Dataset mapRow(ResultSet rs, int rowNum) throws SQLException {
      Dataset dataset = new Dataset();

      String name = rs.getString("name");
      dataset.setName(name.trim());

      String productName = rs.getString("product");

      String productUuid = "";
      if (StringUtils.isNotEmpty(productName)) {
        Product product = productDao.getByName(productName);
        if (Objects.nonNull(product)) {
          productUuid = product.getUuid();
        } else {
          LOGGER.warn("dataset {}, product name {} not exist", name, productName);
        }
      } else {
        LOGGER.warn("dataset {}, product name is empty", name);
      }

      dataset.setProductUuid(productUuid);

      dataset.setPeriod(Integer.valueOf(rs.getString("period")));
      dataset.setSize(Double.valueOf(rs.getString("size")));
      dataset.setContacts(new String[]{rs.getString("connect")});
      dataset.setComment(rs.getString("info"));

      return dataset;
    }

  }

  @Override
  public List<Dataset> gets() {
    String sql = "select concat(dip_categorys.type, '_', dip_access_key.access_key, '_', dip_categorys.dataset) as name, dip_categorys.product, dip_categorys.period, dip_categorys.size, dip_categorys.connect, dip_categorys.info from dip_categorys inner join dip_access_key on dip_categorys.access_id = dip_access_key.id where dip_categorys.trash = 0";

    return consoleJT.query(sql, new ConsoleDatasetRowMapper(productDao));
  }

}
