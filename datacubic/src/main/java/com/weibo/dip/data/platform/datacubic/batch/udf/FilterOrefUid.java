package com.weibo.dip.data.platform.datacubic.batch.udf;

import jodd.util.CollectionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.worker.Sleeper;
import org.apache.spark.sql.api.java.UDF2;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Created by xiaoyu on 2017/4/26.
 */

public class FilterOrefUid implements UDF2<String,String, String> {
    private String path;
    private static Set<String> VESSEL = null;

    public FilterOrefUid(String path) {
        this.path = path;
    }

    private void initSet() throws Exception {
        if (VESSEL == null){
            synchronized(FilterOrefUid.class) {
                if (VESSEL == null){
                    BufferedReader in = null;
                    try {
                        FileSystem fs = FileSystem.get(new URI(path), new Configuration());
                        in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));

                        VESSEL = new HashSet<>();

                        String line;
                        while ((line = in.readLine()) != null) {
                            VESSEL.add(line);
                        }
                    }finally {
                        if (in != null){
                            in.close();
                        }
                    }

                }
            }
        }
    }

    @Override
    public String call(String s, String s2) throws Exception {
        initSet();

        if(VESSEL.contains(s.trim() + "," + s2.trim())){
            return "true";
        }
        return "false";
    }
}
