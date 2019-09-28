package com.weibo.dip.data.platform.datacubic.apmloganalysis.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.lang.exception.ExceptionUtils;

public class DBUtil {

    public class Configure {
        // user name
        public final static String USERNAME = "wbapm";
        // password
        public final static String PASSWORD = "wbapm2018";
        // your database name
        public final static String DBNAME = "wbapm";
        // mysql driver
        public final static String DRIVER = "com.mysql.jdbc.Driver";
        // mysql url
        public final static String URL = "jdbc:mysql://10.85.126.111:3306/" + DBNAME + "?useServerPrepStmts=false&rewriteBatchedStatements=true";
    }

    private static String getInsertSql(String tabName){
        return "INSERT INTO " + tabName + " (flick_category, fingerprints, flick_stacks, is_full, create_time, last_update_time) VALUES (?,?,?,?,?,?)";
    }

    private static String getUpdateSql(String tabName){
        return "UPDATE " + tabName + " set fingerprints=?, flick_stacks=?, is_full=?, last_update_time=? WHERE id=?";
    }

    public static String getAllSql(String tabName){
        return "SELECT * FROM " + tabName + " order by `id` desc";
    }

    public static Map<String, String> iosCategory = new HashMap<>();

    public static Map<String, String> androidCategory = new HashMap<>();

    public static Set<String> newestIosFingerprints = new HashSet<>();

    public static Set<String> newestAndroidFingerprints = new HashSet<>();

    private static Connection connection = null;

    //private static String iosFlickTabName = "ios_flick_test";
    //private static String andrFlickTabName = "android_flick_test";
    private static String iosFlickTabName = "ios_flick_category";
    private static String andrFlickTabName = "android_flick_category";

    static {
        Timer timer = new Timer("flick-category-timer");
        timer.schedule(new FpMapTask("0", iosFlickTabName), 30000l, 40000l);
        timer.schedule(new FpMapTask("0", andrFlickTabName), 40000l, 40000l);
        timer.schedule(new FpMapTask("1", iosFlickTabName), 10000l, 3600000l);
        timer.schedule(new FpMapTask("1", andrFlickTabName), 20000l, 3600000l);
    }

    public DBUtil() {
        if (connection == null) {
            synchronized (Connection.class){
                if (connection == null) {
                    try {
                        Class.forName(Configure.DRIVER);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    try {
                        connection = DriverManager.getConnection(Configure.URL, Configure.USERNAME, Configure.PASSWORD);
                        connection.setAutoCommit(false);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    //已验证
    public int insert(List<String[]> list, String tabName){
        int lineNum = -1;
        try{
            PreparedStatement preStmt = connection.prepareStatement(getInsertSql(tabName));

            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Random random = new Random();
            for(String[] array : list){
                if(array.length != 4){
                    continue;
                }
                int count = array[3].split(",").length;
                preStmt.setString(1, "" + System.currentTimeMillis() + random.nextInt(1000000));
                preStmt.setString(2, array[2]);
                preStmt.setString(3, array[3]);
                preStmt.setString(4, count == 5 ? "true" : "false");
                preStmt.setString(5, sdf.format(new java.util.Date().getTime()));
                preStmt.setString(6, sdf.format(new java.util.Date().getTime()));
                preStmt.addBatch();// 加入批量处理
            }
            int[] res = preStmt.executeBatch();// 执行批量处理
            connection.commit();// 提交

            lineNum = res.length > 0 ? res[0] : -1;
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
        }
        return lineNum;
    }

    public int update(List<String[]> list, String tabName) {
        int lineNum = -1;
        try{
            PreparedStatement preStmt = connection.prepareStatement(getUpdateSql(tabName));
            //"UPDATE " + tabName + " set fingerprints=?, flick_stacks=?, is_full=?, last_update_time=? WHERE id=?";

            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            for(String[] array : list){
                if(array.length != 6){
                    continue;
                }
                preStmt.setString(1, array[2]); // fingerprints
                preStmt.setString(2, array[3]); // flick_stacks
                preStmt.setString(3, array[4]); // is_full
                preStmt.setString(4, sdf.format(new java.util.Date().getTime())); // last_update_time
                preStmt.setString(5, array[5]); // id
                preStmt.addBatch();// 加入批量处理
            }
            int[] res = preStmt.executeBatch();// 执行批量处理
            connection.commit();// 提交

            lineNum = res.length > 0 ? res[0] : -1;
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
        }
        return lineNum;
    }

    public Map<String, String> getCategoryMap(String tabName){

        if(iosFlickTabName.equals(tabName)){
            if(iosCategory.size() == 0){
                return select(tabName);
            }
            return iosCategory;
        } else {
            if(androidCategory.size() == 0){
                return select(tabName);
            }
            return androidCategory;
        }
    }

    public Set<String> getNewestFingerprint(String tabName){

        if(iosFlickTabName.equals(tabName)){
            return newestIosFingerprints;
        } else {
            return newestAndroidFingerprints;
        }
    }

    public Map<String, String> select(String tabName){
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(getAllSql(tabName));

            if(iosFlickTabName.equals(tabName)){
                iosCategory = transformCategoryStructure(rs);
                return iosCategory;
            } else {
                androidCategory = transformCategoryStructure(rs);
                return androidCategory;
            }
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
            return null;
        }
    }

    private Map<String, String> transformCategoryStructure(ResultSet rs){

        Map<String, String> result = new HashMap<>();
        try {
            while(rs.next()){
                String id = rs.getString("id");
                String category = rs.getString("flick_category");
                String fingerprints = rs.getString("fingerprints");
                String stacks = rs.getString("flick_stacks");
                String isFull = rs.getString("is_full");

                String[] fingerprintArr = fingerprints.split(",");
                for(String key : fingerprintArr){
                    result.put(key, id + "#" + category + "#" + fingerprints + "#" + stacks + "#" + isFull);
                }
            }
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
        }
        return result;
    }

    public int delete(List<String> ids, String tabName){
        int lineNum = 0;

        String sql = "delete from " + tabName + " where id=?";

        try{
            PreparedStatement preStmt = connection.prepareStatement(sql);

            for(String id : ids){
                preStmt.setString(1, id);
                preStmt.addBatch();// 加入批量处理
            }
            int[] res = preStmt.executeBatch();// 执行批量处理
            connection.commit();// 提交

            lineNum = res.length > 0 ? res[0] : -1;
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
        }
        return lineNum;
    }


    public void compare(String tabName){

        List<String> ids = new ArrayList<>();

        String sql = "select * FROM " + tabName + " where last_update_time > (select SUBDATE(now(),interval 600 minute))";

        try {
            Statement stmt = connection.createStatement();
            //ResultSet rs = stmt.executeQuery(getAllSql(tabName));
            ResultSet rs = stmt.executeQuery(sql);

            Map<String, String> map = new HashMap<>();
            while(rs.next()){
                //String category = rs.getString("flick_category");
                String id = rs.getString("id");
                String fingerprints = rs.getString("fingerprints");

                String[] fingerprintArr = fingerprints.split(",");
                for(String key : fingerprintArr){
                    if(map.containsKey(key)){
                        System.out.println(map.get(key) + " " + id);
                        ids.add(id);
                        //delete(id);
                    } else {
                        map.put(key, id);
                    }
                }
            }
        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);

        }

        //System.out.println(delete(ids, tabName));
    }

    public void selById(){

        String sql = "SELECT * FROM ios_flick_category order by `id` desc";
        try{
            Statement stmt = connection.createStatement();
            //ResultSet rs = stmt.executeQuery(getAllSql(tabName));
            ResultSet rs = stmt.executeQuery(sql);


            while(rs.next()){
                //String category = rs.getString("flick_category");
                String id = rs.getString("id");
                System.out.println(id);
            }

        } catch (SQLException e) {
            ExceptionUtils.getFullStackTrace(e);
        }

    }

    private static class FpMapTask extends TimerTask {

        private String type;
        private String tabName;

        public FpMapTask(String type, String tabName) {
            this.type = type;
            this.tabName = tabName;
        }

        private Map<String, String> transformCategoryStructure(ResultSet rs){

            Map<String, String> result = new HashMap<>();
            try {
                //int sum = 0;
                while(rs.next()){
                    String id = rs.getString("id");
                    String category = rs.getString("flick_category");
                    String fingerprints = rs.getString("fingerprints");
                    String stacks = rs.getString("flick_stacks");
                    String isFull = rs.getString("is_full");

                    String[] fingerprintArr = fingerprints.split(",");
                    for(String key : fingerprintArr){
                        result.put(key, id + "#" + category + "#" + fingerprints + "#" + stacks + "#" + isFull);
                    }
                    //sum ++;

                }
                //System.out.println("sum" + sum);
            } catch (SQLException e) {
                ExceptionUtils.getFullStackTrace(e);
            }
            return result;
        }

        private String getNewestSql(){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date now = new Date();
            sdf.format(now);
            Date beforeDate = new Date(now.getTime() - 180000);
            String sql = "select * FROM " + tabName + " where last_update_time > \'" + sdf.format(beforeDate) + "\'";

            //System.out.println(sql);
            return sql;
        }

        @Override
        public void run() {

            try {
                Statement stmt = connection.createStatement();

                if("0".equals(type)){

                    ResultSet rs = stmt.executeQuery(getNewestSql());

                    Map<String, String> result = new HashMap<>();
                    List<String> list = new ArrayList<>();

                    //int count = 0;
                    while(rs.next()){
                        String id = rs.getString("id");
                        String category = rs.getString("flick_category");
                        String fingerprints = rs.getString("fingerprints");
                        String stacks = rs.getString("flick_stacks");
                        String isFull = rs.getString("is_full");

                        String[] fingerprintArr = fingerprints.split(",");
                        for(String key : fingerprintArr){
                            result.put(key, id + "#" + category + "#" + fingerprints + "#" + stacks + "#" + isFull);
                            list.add(key);
                        }
                        //count ++ ;
                        //System.out.println(id);
                    }
                    connection.commit();

                    //System.out.println("count: " + count);

                    if(iosFlickTabName.equals(tabName)){
                        iosCategory.putAll(result);
                        newestIosFingerprints.clear();
                        newestIosFingerprints.addAll(list);
                    } else {
                        androidCategory.putAll(result);
                        newestAndroidFingerprints.clear();
                        newestAndroidFingerprints.addAll(list);
                    }

                } else if("1".equals(type)){
                    String sql = "SELECT * FROM " + tabName + " order by `id` desc";
                    ResultSet rs = stmt.executeQuery(sql);
                    if(iosFlickTabName.equals(tabName)){
                        synchronized (iosCategory){
                            iosCategory.clear();
                            iosCategory.putAll(transformCategoryStructure(rs));
                        }
                    } else {
                        synchronized (androidCategory){
                            androidCategory.clear();
                            androidCategory.putAll(transformCategoryStructure(rs));
                        }
                    }
                    connection.commit();
                }

            } catch (SQLException e) {
                ExceptionUtils.getFullStackTrace(e);
            }
        }

    }

    public static void main(String[] args){

        DBUtil util = new DBUtil();

        util.compare("android_flick_category");
        util.compare("ios_flick_category");

        String tabName = "ios_flick_category";
        String sql = "SELECT * FROM " + tabName + " order by `id` desc";

        //System.out.println(sql);

        //util.compare("ios_flick_test");

        //util.selById();

        //Map<String, String> map = util.select(tabName);
        //for(Map.Entry<String, String> entry : map.entrySet()){
        //   System.out.println(entry.getKey());
        //}


    }


}
