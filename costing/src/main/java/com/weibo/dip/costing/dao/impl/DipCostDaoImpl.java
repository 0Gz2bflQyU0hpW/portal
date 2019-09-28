package com.weibo.dip.costing.dao.impl;

import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.dao.DipCostDao;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Created by yurun on 18/4/24.
 */
@Repository
public class DipCostDaoImpl implements DipCostDao {

  @Autowired
  @Qualifier("costJdbcTemplate")
  private JdbcTemplate costJT;

  @Override
  public int add(DipCost dipCost) throws DataAccessException {
    String sql = "insert into dip_cost"
        + "(year, month, product_name, product_uuid, cost_quota, quota_unit, cost_type, percent, descs) "
        + "values(?, ?, ?, ?, ?, ?, ?, ?, ?)";

    KeyHolder keyHolder = new GeneratedKeyHolder();

    return costJT.update(conn -> {
      PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

      stmt.setString(1, dipCost.getYear());
      stmt.setString(2, dipCost.getMonth());
      stmt.setString(3, dipCost.getProductName());
      stmt.setString(4, dipCost.getProductUuid());
      stmt.setDouble(5, dipCost.getCostQuota());
      stmt.setString(6, dipCost.getQuotaUnit());
      stmt.setString(7, dipCost.getCostType());
      stmt.setDouble(8, dipCost.getPercent());
      stmt.setString(9, dipCost.getDescs());

      return stmt;
    }, keyHolder) == 1 ? keyHolder.getKey().intValue() : -1;
  }

}
