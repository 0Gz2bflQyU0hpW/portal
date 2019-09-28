package com.weibo.dip.baishanyundownload.domain;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Domain service.
 *
 * @author yurun
 */
public class DomainService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DomainService.class);

  private static final String BAISHANYUN_DOMAIN_LIST = "https://api.qingcdn.com/v2/domain/list";

  private String token;

  /**
   * Constaruct a DomainService instance.
   *
   * @param token token
   */
  public DomainService(String token) {
    this.token = token;
  }

  /**
   * Get domains.
   *
   * @return domains
   * @throws Exception if http error
   */
  public List<Domain> getDomains() throws Exception {
    List<Domain> domains = new ArrayList<>();

    int pageNumber = 1;
    int totalCount = 0;

    while (true) {
      Map<String, String> param = new HashMap<>();

      param.put("token", token);
      param.put("page_number", String.valueOf(pageNumber));

      String response = HttpClientUtil.doNativeGet(BAISHANYUN_DOMAIN_LIST, param);

      LOGGER.debug("response: {}", response);

      JsonParser parser = new JsonParser();

      JsonObject result = parser.parse(response).getAsJsonObject();

      JsonArray domainArray = result.get("data").getAsJsonObject().get("list").getAsJsonArray();

      for (int index = 0; index < domainArray.size(); index++) {
        JsonObject domainJson = domainArray.get(index).getAsJsonObject();

        String id = domainJson.getAsJsonPrimitive("id").getAsString();
        String domain = domainJson.getAsJsonPrimitive("domain").getAsString();
        String type = domainJson.getAsJsonPrimitive("type").getAsString();
        String status = domainJson.getAsJsonPrimitive("status").getAsString();
        String cname = domainJson.getAsJsonPrimitive("cname").getAsString();
        String icpStatus = domainJson.getAsJsonPrimitive("icp_status").getAsString();
        String icpNum =
            domainJson.get("icp_num").isJsonNull()
                ? ""
                : domainJson.getAsJsonPrimitive("icp_num").getAsString();

        domains.add(new Domain(id, domain, type, status, cname, icpStatus, icpNum));

        totalCount++;
      }

      if (totalCount
          != result.getAsJsonObject("data").getAsJsonPrimitive("total_number").getAsInt()) {
        pageNumber++;
      } else {
        break;
      }
    }

    return domains;
  }
}
