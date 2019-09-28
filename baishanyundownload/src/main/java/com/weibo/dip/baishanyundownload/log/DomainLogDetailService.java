package com.weibo.dip.baishanyundownload.log;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domain log service.
 *
 * @author yurun
 */
public class DomainLogDetailService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainLogDetailService.class);

  private static final String BAISHANYUN_DOMAIN_LOG_LOGS =
      "https://api.qingcdn.com/v2/stat/log/getList";

  private static final FastDateFormat YYYY_MM_DD_HH_MM =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm");

  private String token;
  private int threads;

  /**
   * Create DomainLogService.
   *
   * @param token token
   * @param threads fetcher threads
   */
  public DomainLogDetailService(String token, int threads) {
    this.token = token;
    this.threads = threads;
  }

  private class DomainLogFetcher implements Callable<List<DomainLogDetail>> {
    private String domain;
    private String startTime;
    private String endTime;

    public DomainLogFetcher(String domain, String startTime, String endTime) {
      this.domain = domain;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public List<DomainLogDetail> call() throws Exception {
      List<DomainLogDetail> logDetails = new ArrayList<>();

      LOGGER.debug(
          "get logs for domain: {}, startTime: {}, endTime: {}", domain, startTime, endTime);

      Map<String, String> param = new HashMap<>();

      param.put("token", token);
      param.put("domain", domain);
      param.put("start_time", startTime);
      param.put("end_time", endTime);
      param.put("need_md5", "1");
      param.put("need_size", "1");

      String response = HttpClientUtil.doGet(BAISHANYUN_DOMAIN_LOG_LOGS, param);

      LOGGER.debug("response: {}", response);

      JsonParser jsonParser = new JsonParser();

      JsonObject result = jsonParser.parse(response).getAsJsonObject();

      if (result.getAsJsonPrimitive("code").getAsInt() == 0) {
        JsonArray domainLogDetails = result.getAsJsonArray("data");

        for (int index = 0; index < domainLogDetails.size(); index++) {
          JsonObject domainLogDetail = domainLogDetails.get(index).getAsJsonObject();

          logDetails.add(
              new DomainLogDetail(
                  domainLogDetail.getAsJsonPrimitive("domain").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("date").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("type").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("url").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("md5").getAsString(),
                  domainLogDetail.getAsJsonPrimitive("size").getAsLong()));
        }
      }

      LOGGER.debug(
          "domain: {}, startTime: {}, endTime: {}, logs: {}",
          domain,
          startTime,
          endTime,
          logDetails.size());

      return logDetails;
    }
  }

  public List<DomainLogDetail> getLogs(List<String> domains, Date startTime, Date endTime) {
    return getLogs(domains, YYYY_MM_DD_HH_MM.format(startTime), YYYY_MM_DD_HH_MM.format(endTime));
  }

  /**
   * Get logs for some domains.
   *
   * @param domains domain list
   * @param startTime start time(yyyy-MM-dd HH:mm), include
   * @param endTime end time(yyyy-MM-dd HH:mm), exclude
   * @return domain log list
   */
  public List<DomainLogDetail> getLogs(List<String> domains, String startTime, String endTime) {
    Preconditions.checkState(CollectionUtils.isNotEmpty(domains), "domains can't be empty");
    Preconditions.checkState(StringUtils.isNotEmpty(startTime), "startTime can't be empty");
    Preconditions.checkState(StringUtils.isNotEmpty(endTime), "endTime can't be empty");

    List<DomainLogDetail> domainLogDetails = new ArrayList<>();

    ExecutorService executors = Executors.newFixedThreadPool(threads);

    List<Future<List<DomainLogDetail>>> futures = new ArrayList<>(domains.size());

    domains.forEach(
        domain -> futures.add(executors.submit(new DomainLogFetcher(domain, startTime, endTime))));

    futures.forEach(
        future -> {
          try {
            domainLogDetails.addAll(future.get());
          } catch (Exception e) {
            LOGGER.error("get domain log fetcher error: {}", ExceptionUtils.getStackTrace(e));
          }
        });

    executors.shutdown();

    return domainLogDetails;
  }
}
