package com.weibo.dip.data.platform.datacubic.batch.udf;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF2;

import java.util.Set;

/**
 * Created by xiaoyu on 2017/4/26.
 */

//public class FilterOrefUidTest implements UDF2<String,String, String> {
//    private String path;
//    private static Set<String> VESSEL = null;
//
//    public FilterOrefUidTest(String path) {
//        this.path = path;
//    }
//
//    private void initSet() throws Exception {
//        if (VESSEL == null){
//            synchronized(FilterOrefUidTest.class) {
//                if (VESSEL == null){
//                    BufferedReader in = null;
//                    try {
//                        FileSystem fs = FileSystem.get(new URI(path), new Configuration());
//                        in = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
//
//                        VESSEL = new HashSet<>();
//
//                        String line;
//                        while ((line = in.readLine()) != null) {
//                            VESSEL.add(line);
//                        }
//                    }finally {
//                        if (in != null){
//                            in.close();
//                        }
//                    }
//
//                }
//            }
//        }
//    }
//
//    @Override
//    public String call(String s, String s2) throws Exception {
//        initSet();
//
//        if(VESSEL.contains(s.trim() + "," + s2.trim())){
//            return "true";
//        }
//        return "false";
//    }
//}

public class FilterOrefUidTest implements UDF2<String,String, String> {

    Set<String> set = null;

    public FilterOrefUidTest(Broadcast<Set<String>> broadcast) {
        System.out.println("broadcast size :" + broadcast.value().size() );
//        set = broadcast.value();
//        System.out.println("set size :" + set.size() );
    }

    @Override
    public String call(String s, String s2) throws Exception {
        if(set.contains(s.trim() + "," + s2.trim())){
            return "true";
        }
        return "false";
    }
}
