package com.weibo.dip.costing.cost;

import com.google.common.base.Preconditions;
import com.weibo.dip.costing.bean.Dataset;
import com.weibo.dip.costing.bean.DipCost;
import com.weibo.dip.costing.bean.MRLog;
import com.weibo.dip.costing.bean.Product;
import com.weibo.dip.costing.bean.Server.Role;
import com.weibo.dip.costing.bean.Server.Type;
import com.weibo.dip.costing.service.DatasetService;
import com.weibo.dip.costing.service.MRLogService;
import com.weibo.dip.costing.service.ProductService;
import com.weibo.dip.costing.service.ServerService;
import com.weibo.dip.costing.util.DatetimeUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * Created by yurun on 18/4/23.
 */
@Component
public class HadoopCost implements Cost {

  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopCost.class);

  @Autowired
  private ProductService productService;

  @Autowired
  private DatasetService datasetService;

  @Autowired
  private ServerService serverService;

  @Autowired
  private MRLogService mrLogService;

  private List<DipCost> storageCost(String year, String month) {
    int server = serverService.servers();

    Map<String, Integer> productUuidToServers = serverService.servers(Role.HADOOP, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(productUuidToServers),
        "hadoop storage servers is empty");

    int hadoopServer = productUuidToServers.values().stream().reduce((a, b) -> a + b).orElse(0);

    double hadoopServerPercent = hadoopServer / 1.0 / server;

    List<Dataset> datasets = datasetService.gets();
    Preconditions.checkState(!CollectionUtils.isEmpty(datasets), "datasets is empty");

    List<Product> products = productService.gets();
    Preconditions.checkState(!CollectionUtils.isEmpty(products), "products is empty");

    Map<String, Product> uuidToProducts = products.stream()
        .collect(Collectors.toMap(Product::getUuid, product -> product));

    Map<String, Long> productUuidToSize = datasets.stream()
        .filter(
            dataset -> {
              if (!uuidToProducts.containsKey(dataset.getProductUuid())) {
                LOGGER.warn("can't find product info for dataset {}", dataset.getName());

                return false;
              }

              return true;
            })
        .collect(
            Collectors.toMap(
                Dataset::getProductUuid,
                dataset -> datasetService.size(dataset.getName()),
                (a, b) -> a + b));

    productUuidToSize.remove(DIP_PRODUCT_UUID);

    long quantum = productUuidToSize.values().stream().reduce((a, b) -> a + b).orElse(0L);

    List<DipCost> costs = new ArrayList<>();

    productUuidToSize.entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .forEach(
            entry -> {
              String productUuid = entry.getKey();
              String productName = uuidToProducts.get(productUuid).getName();

              long costQuota = entry.getValue();

              double percent = (costQuota / 1.0 / quantum) * hadoopServerPercent * STORAGE_FACTOR;

              DipCost cost = new DipCost();

              cost.setYear(year);
              cost.setMonth(month);
              cost.setProductName(productName);
              cost.setProductUuid(productUuid);
              cost.setCostQuota(costQuota);
              cost.setQuotaUnit("B");
              cost.setCostType("dip_hdfs_storage");
              cost.setPercent(percent);
              cost.setDescs("");

              costs.add(cost);
            });

    return costs;
  }

  private List<DipCost> computeCost(String year, String month) {
    int server = serverService.servers();

    /*
      Share
     */
    Map<String, Integer> shareProductUuidToServers = serverService.servers(Role.HADOOP, Type.SHARE);
    Preconditions.checkState(
        MapUtils.isNotEmpty(shareProductUuidToServers),
        "hadoop compute servers is empty");

    int hadoopServer = shareProductUuidToServers.values().stream()
        .reduce((a, b) -> a + b).orElse(0);

    double hadoopServerPercent = hadoopServer / 1.0 / server;

    Date beginTime = DatetimeUtil.monthBegin(year, month);
    Date endTime = DatetimeUtil.monthEnd(year, month);

    List<MRLog> mrLogs = mrLogService.gets(beginTime, endTime);
    Preconditions.checkState(!CollectionUtils.isEmpty(mrLogs), "mr logs is empty");

    Map<String, Dataset> nameToDatasets = datasetService.gets().stream()
        .collect(Collectors.toMap(Dataset::getName, dataset -> dataset));

    Map<String, Product> uuidToProducts = productService.gets().stream()
        .collect(Collectors.toMap(Product::getUuid, product -> product));

    Map<String, Long> productUuidToSize = mrLogs.stream()
        .filter(mrLog -> mrLog.getJobname().startsWith("select_job_")
            || mrLog.getJobname().startsWith("hive_"))
        .filter(
            mrLog -> {
              if (ArrayUtils.isEmpty(mrLog.getCategories())
                  || StringUtils.isEmpty(mrLog.getCategories()[0])) {
                LOGGER.warn("category is empty for job {}", mrLog.getJobname());

                return false;
              }

              return true;
            })
        .filter(
            mrLog -> {
              if (!nameToDatasets.containsKey(mrLog.getCategories()[0])) {
                LOGGER.warn("can't find dataset for job {}", mrLog.getJobname());

                return false;
              }

              return true;
            })
        .filter(mrLog -> {
          if (!uuidToProducts
              .containsKey(nameToDatasets.get(mrLog.getCategories()[0]).getProductUuid())) {
            LOGGER.warn("can't find product for job {}", mrLog.getJobname());

            return false;
          }

          return true;
        })
        .collect(
            Collectors.toMap(
                mrLog -> nameToDatasets.get(mrLog.getCategories()[0]).getProductUuid(),
                MRLog::getHdfsBytesRead,
                (a, b) -> a + b
            ));

    productUuidToSize.remove(DIP_PRODUCT_UUID);

    long quantum = productUuidToSize.values().stream().reduce((a, b) -> a + b).orElse(0L);

    List<DipCost> costs = new ArrayList<>();

    productUuidToSize.entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .forEach(
            entry -> {
              String productUuid = entry.getKey();
              String productName = uuidToProducts.get(productUuid).getName();

              double costQuota = entry.getValue();

              double percent = (costQuota / quantum) * hadoopServerPercent * COMPUTE_FACTOR;

              DipCost cost = new DipCost();

              cost.setYear(year);
              cost.setMonth(month);
              cost.setProductName(productName);
              cost.setProductUuid(productUuid);
              cost.setCostQuota(entry.getValue());
              cost.setQuotaUnit("B");
              cost.setCostType("dip_hadoop_analyzes");
              cost.setPercent(percent);
              cost.setDescs("");

              costs.add(cost);
            });

    /*
      Exclusive
     */
    Map<String, Integer> exclusiveProductUuidToServers =
        serverService.servers(Role.HADOOP, Type.EXCLUSIVE);
    if (MapUtils.isNotEmpty(exclusiveProductUuidToServers)) {
      exclusiveProductUuidToServers.entrySet().stream()
          .filter(entry -> {
            if (!uuidToProducts.containsKey(entry.getKey())) {
              LOGGER.warn("can't find product for uuid {}", entry.getKey());

              return false;
            }

            return true;
          })
          .forEach(
              entry -> {
                String productUuid = entry.getKey();
                int productServer = entry.getValue();

                DipCost cost = new DipCost();

                cost.setYear(year);
                cost.setMonth(month);
                cost.setProductName(productService.getByUuid(productUuid).getName());
                cost.setProductUuid(productUuid);
                cost.setCostQuota(productServer);
                cost.setQuotaUnit("servers");
                cost.setCostType("dip_hadoop_analyzes_exclusive");
                cost.setPercent(productServer / 1.0 / server);
                cost.setDescs("");

                costs.add(cost);
              });
    }

    return costs;
  }

  @Override
  public List<DipCost> cost(String year, String month) {
    List<DipCost> costs = new ArrayList<>();

    List<DipCost> storageCosts = storageCost(year, month);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(storageCosts),
        "hadoop storage costs is empty");

    List<DipCost> computeCosts = computeCost(year, month);
    Preconditions.checkState(
        !CollectionUtils.isEmpty(computeCosts),
        "hadoop compute costs is empty");

    costs.addAll(storageCosts);
    costs.addAll(computeCosts);

    return costs;
  }

}
