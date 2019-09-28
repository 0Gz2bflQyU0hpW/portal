package com.weibo.dip.data.platform.datacubic.apmloganalysis;

import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class SqlTest {

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
        public final static String URL = "jdbc:mysql://10.85.126.111:3306/" + DBNAME;

                //+ "?useServerPrepStmts=false&rewriteBatchedStatements=true";
    }

    private static Connection connection = null;

    private static String iosFlickTabName = "ios_flick_category";
    private static String andrFlickTabName = "android_flick_category";

    static {
        Timer timer = new Timer("flick-category-timer");
        timer.schedule(new FpMapTask("0", andrFlickTabName), 20000l, 20000l);
    }

    private static Connection getConnection(){
        if (connection == null) {
            System.out.println("连接初始化...");
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
        return connection;
    }


    private static class FpMapTask extends TimerTask {

        private String type;
        private String tabName;

        public FpMapTask(String type, String tabName) {
            this.type = type;
            this.tabName = tabName;
        }

        private String getNewestSql(){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date now = new Date();
            sdf.format(now);
            Date beforeDate = new Date(now.getTime() - 300000);
            //String sql = "select * FROM " + tabName + " where last_update_time > \'" + sdf.format(beforeDate) + "\'";

            String sql = "select * FROM " + tabName + " where last_update_time > \'" + sdf.format(beforeDate) + "\'"
                    + " AND last_update_time < \'" + sdf.format(now) + "\'";

            //System.out.println(sql);
            return sql;
        }

        @Override
        public void run() {

            try {
                connection = SqlTest.getConnection();
                Statement stmt = connection.createStatement();

                if("0".equals(type)){
                    long sta = System.currentTimeMillis();
                    String sql = getNewestSql();
                    System.out.println(sql);

                    ResultSet rs = stmt.executeQuery(sql);

                    int count = 0;
                    while(rs.next()){
                        String id = rs.getString("id");
                        count ++ ;
                        System.out.println(id);
                    }
                    connection.close();
                    System.out.println("count: " + count);

                    long end = System.currentTimeMillis();
                    System.out.println("---" + (end - sta));
                }
            } catch (SQLException e) {
                ExceptionUtils.getFullStackTrace(e);
            }
        }

    }


    public static void main(String[] args){


    }



}
