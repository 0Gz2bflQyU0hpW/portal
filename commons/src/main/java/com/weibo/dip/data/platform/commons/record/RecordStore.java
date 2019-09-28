package com.weibo.dip.data.platform.commons.record;

import com.weibo.dip.data.platform.commons.util.GsonUtil;
import com.weibo.dip.data.platform.commons.util.HttpClientUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by yurun on 18/1/22.
 */
public class RecordStore {

  private static final String SUMMON_REST = "http://api.dip.weibo.com:9083/summon/writes";

  private static final String RECORDS = "records";

  private RecordStore() {

  }

  public static void store(Record record) throws Exception {
    if (Objects.isNull(record)) {
      return;
    }

    store(Collections.singletonList(record));
  }

  public static void store(List<Record> records) throws Exception {
    if (CollectionUtils.isEmpty(records)) {
      return;
    }

    HttpClientUtil.doPost(SUMMON_REST, Collections.singletonMap(RECORDS, GsonUtil.toJson(records)));
  }

}
