package com.weibo.dip.data.platform.datacubic.streaming.udf;

import com.sina.dip.iplibrary.Location;
import com.sina.dip.iplibrary.MemoryIpToLocationService;
import org.apache.spark.sql.api.java.UDF1;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yurun on 17/1/19.
 */
public class IpToLocation implements UDF1<String, Map<String, String>> {

    private static final String LIBRARY_DIR = "/data0/dipplat/software/systemfile/iplibrary";

    private static final MemoryIpToLocationService IP_TO_LOCATION_SERVICE;

    private static final Location OTHER = new Location("Other", "Other", "Other", "Other", "Other", "Other", "Other");

    private static final String COUNTRY = "country";

    private static final String PROVINCE = "province";

    private static final String CITY = "city";

    private static final String DISTRICT = "district";

    private static final String ISP = "isp";

    private static final String TYPE = "type";

    private static final String DESC = "desc";

    static {
        try {
            IP_TO_LOCATION_SERVICE = MemoryIpToLocationService.getInstance(LIBRARY_DIR);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Map<String, String> call(String ip) throws Exception {
        Location location = IP_TO_LOCATION_SERVICE.toLocation(ip);
        if (location == null) {
            location = OTHER;
        }

        Map<String, String> result = new HashMap<>();

        result.put(COUNTRY, location.getCountry());
        result.put(PROVINCE, location.getProvince());
        result.put(CITY, location.getCity());
        result.put(DISTRICT, location.getDistrict());
        result.put(ISP, location.getIsp());
        result.put(TYPE, location.getType());
        result.put(DESC, location.getDesc());

        return result;
    }

}
