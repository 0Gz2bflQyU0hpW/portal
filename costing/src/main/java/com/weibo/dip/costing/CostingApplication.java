package com.weibo.dip.costing;

import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.cost.Cost;
import com.weibo.dip.costing.cost.HadoopCost;
import com.weibo.dip.costing.cost.KafkaCost;
import com.weibo.dip.costing.cost.StormCost;
import com.weibo.dip.costing.cost.StreamingCost;
import com.weibo.dip.costing.cost.SummonCost;
import com.weibo.dip.costing.service.DipCostService;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Created by yurun on 18/4/18.
 */
@SpringBootApplication
public class CostingApplication implements ApplicationContextAware, CommandLineRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(CostingApplication.class);

  private static final Map<Role, Class<? extends Cost>> COSTS = new HashMap<>();

  static {
    COSTS.put(Role.HADOOP, HadoopCost.class);
    COSTS.put(Role.STREAMING, StreamingCost.class);
    COSTS.put(Role.KAFKA, KafkaCost.class);
    COSTS.put(Role.SUMMON, SummonCost.class);
    COSTS.put(Role.STORM, StormCost.class);
  }

  private ApplicationContext context;

  @Autowired
  private DipCostService dipCostService;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.context = applicationContext;
  }

  @Override
  public void run(String... args) throws Exception {
    String year;
    String month;

    if (ArrayUtils.isNotEmpty(args) && args.length == 2) {
      year = args[0];
      month = args[1];
    } else {
      SimpleDateFormat yearSdf = new SimpleDateFormat("yyyy");
      SimpleDateFormat monthSdf = new SimpleDateFormat("MM");

      Calendar calendar = Calendar.getInstance();

      calendar.add(Calendar.MONTH, -1);

      year = yearSdf.format(calendar.getTime());
      month = monthSdf.format(calendar.getTime());
    }

    LOGGER.info("year: {}, month: {}", year, month);

    for (Role role : Role.values()) {
      Class<? extends Cost> costClass = COSTS.get(role);
      if (Objects.isNull(costClass)) {
        LOGGER.warn("can't find cost class for {}", role.name());

        continue;
      }

      Cost cost = context.getBean(costClass);

      List<DipCost> costs = cost.cost(year, month);
      if (CollectionUtils.isEmpty(costs)) {
        LOGGER.warn("{} cost is empty", role.name());

        continue;
      }

      for (DipCost dipCost : costs) {
        dipCostService.add(dipCost);

        LOGGER.info("costtype: {}, product: {}, percent: {}",
            dipCost.getCostType(), dipCost.getProductName(), dipCost.getPercent());
      }
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(CostingApplication.class, args);
  }

}
