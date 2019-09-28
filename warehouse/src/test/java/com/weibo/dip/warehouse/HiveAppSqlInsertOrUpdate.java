package com.weibo.dip.warehouse;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Objects;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/** @author yurun */
public class HiveAppSqlInsertOrUpdate {
  public static String getSqls() throws Exception {
    InputStream in = null;

    try {
      in = HiveAppSqlInsertOrUpdate.class.getClassLoader().getResourceAsStream("sqls");

      List<String> sqls = IOUtils.readLines(in, Charsets.UTF_8);

      return StringUtils.join(sqls, "\n");
    } finally {
      if (Objects.nonNull(in)) {
        in.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String name = "video_plays_distribution";
    String sqls = getSqls();

    String driver = "com.mysql.jdbc.Driver";
    String url =
        "jdbc:mysql://d136092.innet.dip.weibo.com:3307/scheduler2?&useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String username = "root";
    String password = "mysqladmin";

    String sql = "insert into hiveapps (name, sqls) values (?, ?) on duplicate key update sqls = ?";

    Connection conn;
    PreparedStatement stmt;

    Class.forName("com.mysql.jdbc.Driver");

    conn = DriverManager.getConnection(url, username, password);

    stmt = conn.prepareStatement(sql);
    stmt.setString(1, name);
    stmt.setString(2, sqls);
    stmt.setString(3, sqls);

    stmt.executeUpdate();

    stmt.close();
    conn.close();
  }
}
