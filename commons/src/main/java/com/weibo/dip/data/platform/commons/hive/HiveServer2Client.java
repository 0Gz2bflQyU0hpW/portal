package com.weibo.dip.data.platform.commons.hive;

import com.google.common.base.Preconditions;
import com.weibo.dip.data.platform.commons.selector.HashSelector;
import com.weibo.dip.data.platform.commons.selector.Selector;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveServer2 client.
 *
 * @author yurun
 */
public class HiveServer2Client implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveServer2Client.class);

  private static final String DRIVER = "org.apache.hive.jdbc.HiveDriver";

  private static final Pattern DDL_PATTERN =
      Pattern.compile("(CREATE|DROP|TRUNCATE|ALTER|GRANT|REVOKE)\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern DML_PATTERN =
      Pattern.compile(
          "(USE|SET|LOAD|INSERT|UPDATE|DELETE|EXPORT|IMPORT)\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern DQL_PATTERN =
      Pattern.compile("(SHOW|DESCRIBE|EXPLAIN|SELECT)\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern SELECT_PATTERN =
      Pattern.compile("SELECT\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern CTAS_PATTERN =
      Pattern.compile("CREATE\\s+TABLE\\s+AS\\s+SELECT\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern INSERT_SELECT_PATTERN =
      Pattern.compile("INSERT\\s+.*\\s+SELECT\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern INSERT_SELECT_EXTENSION_PATTERN =
      Pattern.compile("FROM\\s+.*\\s+INSERT\\s+.*\\s+SELECT\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern LOAD_DATA_LOCAL_PATTERN =
      Pattern.compile("LOAD\\s+DATA\\s+LOCAL\\s+INPATH\\s+.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern INSERT_OVERWRITE_LOCAL_PATTERN =
      Pattern.compile("INSERT\\s+OVERWRITE\\s+DIRECTORY\\s+.*", Pattern.CASE_INSENSITIVE);

  /**
   * Is data definition language.
   *
   * @param sql sql
   * @return true if sql is ddl
   */
  public static boolean isDdl(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return DDL_PATTERN.matcher(sql).matches();
  }

  /**
   * Is data manipulation language.
   *
   * @param sql sql
   * @return true if sql is dml
   */
  public static boolean isDml(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return DML_PATTERN.matcher(sql).matches();
  }

  /**
   * Is data query language.
   *
   * @param sql sql
   * @return true if sql is dql
   */
  public static boolean isDql(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return DQL_PATTERN.matcher(sql).matches();
  }

  /**
   * Is select language.
   *
   * @param sql sql
   * @return true if sql is select
   */
  public static boolean isSelect(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return SELECT_PATTERN.matcher(sql).matches();
  }

  /**
   * Is create table as select language.
   *
   * @param sql sql
   * @return true if sql is 'create table as select ...'
   */
  public static boolean isCreateTableAsSelect(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return CTAS_PATTERN.matcher(sql).matches();
  }

  /**
   * Is insert select language.
   *
   * @param sql sql
   * @return true if sql is 'insert select ...'
   */
  public static boolean isInsertSelect(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return INSERT_SELECT_PATTERN.matcher(sql).matches();
  }

  /**
   * Is insert select extension language.
   *
   * @param sql sql
   * @return true if sql is 'insert select extension ...'
   */
  public static boolean isInsertSelectExtension(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return INSERT_SELECT_EXTENSION_PATTERN.matcher(sql).matches();
  }

  /**
   * Is load data local language.
   *
   * @param sql sql
   * @return true if sql is 'load data local ...'
   */
  public static boolean isLoadDataLocal(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return LOAD_DATA_LOCAL_PATTERN.matcher(sql).matches();
  }

  /**
   * Is insert overwrite local language.
   *
   * @param sql sql
   * @return true if sql is 'insert overwrite local language ...'
   */
  public static boolean isInsertOverwriteLocal(String sql) {
    if (StringUtils.isEmpty(sql)) {
      return false;
    }

    return INSERT_OVERWRITE_LOCAL_PATTERN.matcher(sql).matches();
  }

  private Connection conn;
  private Statement stmt;
  private ResultSet rs;

  private HiveServer2Client(String[] urls, int maxRetries, String username, String passwd)
      throws Exception {
    LOGGER.info("urls: " + Arrays.toString(urls));
    Preconditions.checkState(ArrayUtils.isNotEmpty(urls), "urls must be specifield");

    LOGGER.info("maxRetries: " + maxRetries);
    if (urls.length < maxRetries) {
      maxRetries = urls.length;
    }

    LOGGER.info("username: " + username);
    LOGGER.info("passwd: " + passwd);

    Class.forName(DRIVER);

    Selector<String> selector = new HashSelector<>(urls);

    Set<String> selected = new HashSet<>();

    for (int retry = 1; retry <= maxRetries; retry++) {
      String url;

      do {
        url = selector.select();
      } while (selected.contains(url));

      selected.add(url);

      LOGGER.info("retry(" + retry + ") conn to " + url);

      try {
        this.conn = DriverManager.getConnection(url, username, passwd);
      } catch (Exception e) {
        LOGGER.error("get connection({}) error: {}", retry, ExceptionUtils.getFullStackTrace(e));

        if (retry == maxRetries) {
          throw new RuntimeException("retry exceeds limit(" + maxRetries + ")");
        }
      }
    }

    this.stmt = this.conn.createStatement();
  }

  public HiveServer2Client(String url) throws Exception {
    this(url, "", "");
  }

  public HiveServer2Client(String url, String username, String passwd) throws Exception {
    this(new String[] {url}, 1, username, passwd);
  }

  public void execute(String sql) throws SQLException {
    stmt.execute(sql);
  }

  public QueryResult executeQuery(String sql) throws SQLException {
    return executeQuery(sql, Integer.MAX_VALUE);
  }

  /**
   * Execute query.
   *
   * @param sql sql
   * @param max limit query result
   * @return query result
   * @throws SQLException if execute sql error
   */
  public QueryResult executeQuery(String sql, int max) throws SQLException {
    QueryResult result;

    try {
      rs = stmt.executeQuery(sql);

      ResultSetMetaData metaData = rs.getMetaData();

      result = new QueryResult();

      result.setType(QueryResult.Type.ALL);

      result.setColumns(metaData.getColumnCount());

      List<String> columnNames = new ArrayList<>(metaData.getColumnCount());
      List<String> columnTypes = new ArrayList<>(metaData.getColumnCount());

      for (int index = 1; index <= metaData.getColumnCount(); index++) {
        columnNames.add(metaData.getColumnName(index));
        columnTypes.add(metaData.getColumnTypeName(index));
      }

      result.setColumnNames(columnNames.toArray(new String[metaData.getColumnCount()]));
      result.setColumnTypes(columnTypes.toArray(new String[metaData.getColumnCount()]));

      List<List<Object>> datas = new ArrayList<>();

      int count = 0;

      while (rs.next()) {
        if (++count > max) {
          result.setType(QueryResult.Type.PART);
          break;
        }

        List<Object> row = new ArrayList<>(metaData.getColumnCount());

        for (int index = 1; index <= metaData.getColumnCount(); index++) {
          row.add(rs.getObject(index));
        }

        datas.add(row);
      }

      result.setDatas(datas);
    } catch (SQLException e) {
      String msg = e.getMessage();

      if (msg.equals("The query did not generate a result set!")) {
        result = null;
      } else {
        throw e;
      }
    } finally {
      if (rs != null) {
        rs.close();
        rs = null;
      }
    }

    return result;
  }

  /**
   * Create database.
   *
   * @param ifNotExists create db with 'if not exists'
   * @param db db name
   * @param comment comment
   * @param location location
   * @param properties properties
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void createDb(
      boolean ifNotExists,
      String db,
      String comment,
      String location,
      Map<String, String> properties)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    StringBuilder buffer = new StringBuilder();

    buffer.append("CREATE DATABASE");

    if (ifNotExists) {
      buffer.append(" IF NOT EXISTS");
    }

    buffer.append(" ").append(db);

    if (StringUtils.isNotEmpty(comment)) {
      buffer.append(" COMMENT '").append(comment).append("'");
    }

    if (StringUtils.isNotEmpty(location)) {
      buffer.append(" LOCATION '").append(location).append("'");
    }

    if (MapUtils.isNotEmpty(properties)) {
      buffer.append(" WITH DBPROPERTIES (");

      int count = 0;

      for (Entry<String, String> entry : properties.entrySet()) {
        buffer
            .append("'")
            .append(entry.getKey())
            .append("'='")
            .append(entry.getValue())
            .append("'");

        if (++count < properties.size()) {
          buffer.append(", ");
        }
      }

      buffer.append(")");
    }

    execute(buffer.toString());
  }

  public void createDb(String db) throws RuntimeException, SQLException {
    createDb(false, db, null, null, null);
  }

  /**
   * Drop database.
   *
   * @param ifExists drop database with 'if exists'
   * @param db db name
   * @param restrict restrict mode
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void dropDb(boolean ifExists, String db, boolean restrict)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    StringBuilder buffer = new StringBuilder();

    buffer.append("DROP DATABASE");

    if (ifExists) {
      buffer.append(" IF EXISTS");
    }

    buffer.append(" ").append(db);

    if (restrict) {
      buffer.append(" RESTRICT");
    } else {
      buffer.append(" CASCADE");
    }

    execute(buffer.toString());
  }

  /**
   * Alter db properties.
   *
   * @param db db name
   * @param properties db properties
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void alterDbProperties(String db, Map<String, String> properties)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    if (MapUtils.isEmpty(properties)) {
      LOGGER.warn("properties is empty, return");
      return;
    }

    StringBuilder buffer = new StringBuilder();

    buffer.append("ALTER DATABASE ").append(db).append(" SET DBPROPERTIES (");

    int count = 0;

    for (Entry<String, String> entry : properties.entrySet()) {
      buffer.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("'");

      if (++count < properties.size()) {
        buffer.append(", ");
      }
    }

    buffer.append(")");

    execute(buffer.toString());
  }

  /**
   * Alter db owner.
   *
   * @param db db name
   * @param owner db owner
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void alterDbOwner(String db, String owner) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(owner), "owner must be specified");

    execute("ALTER DATABASE " + db + " SET OWNER USER " + owner);
  }

  /**
   * Alter db role.
   *
   * @param db db name
   * @param role db role
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void alterDbRole(String db, String role) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(role), "role must be specified");

    execute("ALTER DATABASE " + db + " SET OWNER ROLE " + role);
  }

  /**
   * Use db.
   *
   * @param db db name
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void useDb(String db) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    execute("USE " + db);
  }

  /**
   * Show databases.
   *
   * @return databases
   * @throws SQLException if execute sql error
   */
  public List<String> showDbs() throws SQLException {
    QueryResult result = executeQuery("SHOW DATABASES");
    if (Objects.isNull(result)) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    List<String> dbs = new ArrayList<>(rows.size());

    for (List<Object> row : rows) {
      dbs.add((String) row.get(0));
    }

    return dbs;
  }

  /**
   * Exist db.
   *
   * @param db db name
   * @return true if db exist, else false
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public boolean existDb(String db) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    List<String> dbs = showDbs();
    if (CollectionUtils.isEmpty(dbs)) {
      return false;
    }

    return dbs.contains(db);
  }

  /**
   * Show tables.
   *
   * @param db db name
   * @param regex table regex
   * @return tables
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public List<String> showTables(String db, String regex) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");

    if (StringUtils.isEmpty(regex)) {
      regex = "*";
    }

    QueryResult result = executeQuery("SHOW TABLES IN " + db + " '" + regex + "'");
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    List<String> tables = new ArrayList<>(rows.size());

    for (List<Object> row : rows) {
      tables.add((String) row.get(0));
    }

    return tables;
  }

  public List<String> showTables(String db) throws RuntimeException, SQLException {
    return showTables(db, "*");
  }

  /**
   * Exist table.
   *
   * @param db db name
   * @param table table name
   * @return true if table exist in db, else false
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public boolean existTable(String db, String table) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    List<String> tables = showTables(db);
    if (CollectionUtils.isEmpty(tables)) {
      return false;
    }

    return tables.contains(table);
  }

  /**
   * Show create table.
   *
   * @param db db name
   * @param table table name
   * @return create table sql
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public List<String> showCreateTable(String db, String table)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    QueryResult result = executeQuery("SHOW CREATE TABLE " + db + "." + table);

    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();

    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    List<String> sqls = new ArrayList<>();

    for (List<Object> row : rows) {
      sqls.add((String) row.get(0));
    }

    return sqls;
  }

  /**
   * Get table location.
   *
   * @param db db name
   * @param table table name
   * @return table location
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public String getTableLocation(String db, String table) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    QueryResult result = executeQuery("DESCRIBE FORMATTED " + db + "." + table);
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    String location = null;

    for (List<Object> row : rows) {
      if (row.get(0).equals("Location:           ")) {
        location = (String) row.get(1);

        break;
      }
    }

    return location;
  }

  private String getPartitionSpec(Map<String, String> specs) {
    List<String> parts = new ArrayList<>(specs.size());

    for (Entry<String, String> entry : specs.entrySet()) {
      String name = entry.getKey();
      String value = entry.getValue();

      String part = name;
      part += "=";
      part += ("'" + value + "'");

      parts.add(part);
    }

    return StringUtils.join(parts, ", ");
  }

  /**
   * Add partition.
   *
   * @param ifNotExists add partition if not exists
   * @param db db name
   * @param table table name
   * @param specs partition specs
   * @param location partition location
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void addPartition(
      boolean ifNotExists, String db, String table, Map<String, String> specs, String location)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");
    Preconditions.checkState(MapUtils.isNotEmpty(specs), "specs must be specified");

    StringBuilder sql = new StringBuilder();

    sql.append("ALTER TABLE ").append(table).append(" ADD");

    if (ifNotExists) {
      sql.append(" IF NOT EXISTS");
    }

    sql.append(" PARTITION (").append(getPartitionSpec(specs)).append(")");

    if (StringUtils.isNotEmpty(location)) {
      sql.append(" LOCATION '").append(location).append("'");
    }

    useDb(db);

    execute(sql.toString());
  }

  public void addPartition(String db, String table, Map<String, String> specs, String location)
      throws SQLException {
    addPartition(false, db, table, specs, location);
  }

  public void addPartition(String db, String table, Map<String, String> specs) throws SQLException {
    addPartition(false, db, table, specs, null);
  }

  /**
   * Drop partition.
   *
   * @param ifExists drop partition if exists
   * @param db db name
   * @param table table name
   * @param specs partition specs
   * @throws RuntimeException if run error
   * @throws SQLException if execute sql error
   */
  public void dropPartition(boolean ifExists, String db, String table, Map<String, String> specs)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");
    Preconditions.checkState(MapUtils.isNotEmpty(specs), "specs must be specified");

    StringBuilder sql = new StringBuilder();

    sql.append("ALTER TABLE ").append(table).append(" DROP");

    if (ifExists) {
      sql.append(" IF EXISTS");
    }

    sql.append(" PARTITION (").append(getPartitionSpec(specs)).append(")");

    useDb(db);

    execute(sql.toString());
  }

  public void dropPartition(String db, String table, Map<String, String> specs)
      throws SQLException {
    dropPartition(false, db, table, specs);
  }

  public void dropPartitionIfExists(String db, String table, Map<String, String> specs)
      throws SQLException {
    dropPartition(true, db, table, specs);
  }

  /**
   * Show partitions.
   *
   * @param db db name
   * @param table table name
   * @return table partitions
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public List<Map<String, String>> showPartitions(String db, String table)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    QueryResult result = executeQuery("SHOW PARTITIONS " + db + "." + table);
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    List<Map<String, String>> partitions = new ArrayList<>(rows.size());

    for (List<Object> row : rows) {
      String value = (String) row.get(0);

      Map<String, String> specs = new HashMap<>();

      String[] parts = value.split("/", -1);

      for (String part : parts) {
        String[] words = part.split("=", -1);

        specs.put(words[0], words[1]);
      }

      partitions.add(specs);
    }

    return partitions;
  }

  /**
   * Exist partition.
   *
   * @param db db name
   * @param table table name
   * @param specs partition specs
   * @return true if partition exist, else false
   * @throws RuntimeException if run error
   * @throws SQLException if execute sql error
   */
  public boolean existPartition(String db, String table, Map<String, String> specs)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");
    Preconditions.checkState(MapUtils.isNotEmpty(specs), "specs must be specified");

    List<Map<String, String>> partitions = showPartitions(db, table);

    if (CollectionUtils.isEmpty(partitions)) {
      return false;
    }

    return partitions.contains(specs);
  }

  /**
   * Get partition location.
   *
   * @param db db name
   * @param table table name
   * @param specs partition specs
   * @return partition location
   * @throws RuntimeException if run error
   * @throws SQLException if execute sql error
   */
  public String getPartitionLocation(String db, String table, Map<String, String> specs)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");
    Preconditions.checkState(MapUtils.isNotEmpty(specs), "specs must be specified");

    QueryResult result =
        executeQuery(
            "DESCRIBE FORMATTED "
                + db
                + "."
                + table
                + " PARTITION ("
                + getPartitionSpec(specs)
                + ")");
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    String location = null;

    for (List<Object> row : rows) {
      if (row.get(0).equals("Location:           ")) {
        location = (String) row.get(1);

        break;
      }
    }

    return location;
  }

  /**
   * Get columns.
   *
   * @param db db name
   * @param table table name
   * @return table columns
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public List<Column> getColumns(String db, String table) throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    QueryResult result = executeQuery("desc " + db + "." + table);
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    List<Column> columns = new ArrayList<>(rows.size());

    for (List<Object> row : rows) {
      String colName = (String) row.get(0);
      if (StringUtils.isEmpty(colName)) {
        break;
      }

      String dataType = (String) row.get(1);
      String comment = (String) row.get(2);

      columns.add(new Column(colName, dataType, comment));
    }

    return columns;
  }

  /**
   * Drop table.
   *
   * @param ifExists drop table if exists
   * @param db db name
   * @param table table name
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public void dropTable(boolean ifExists, String db, String table)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    StringBuilder buffer = new StringBuilder();

    buffer.append("DROP TABLE");

    if (ifExists) {
      buffer.append(" IF EXISTS");
    }

    buffer.append(" ").append(db).append(".").append(table);

    execute(buffer.toString());
  }

  public void dropTable(String db, String table) throws SQLException {
    dropTable(false, db, table);
  }

  public void dropTableIfExists(String db, String table) throws SQLException {
    dropTable(true, db, table);
  }

  /**
   * Show table properties.
   *
   * @param db db name
   * @param table table name
   * @return table properties
   * @throws RuntimeException if params error
   * @throws SQLException if execute sql error
   */
  public Map<String, String> showTblProperties(String db, String table)
      throws RuntimeException, SQLException {
    Preconditions.checkState(StringUtils.isNotEmpty(db), "db must be specified");
    Preconditions.checkState(StringUtils.isNotEmpty(table), "table must be specified");

    useDb(db);

    QueryResult result = executeQuery("SHOW TBLPROPERTIES " + table);
    if (result == null) {
      return null;
    }

    List<List<Object>> rows = result.getDatas();
    if (CollectionUtils.isEmpty(rows)) {
      return null;
    }

    Map<String, String> properties = new HashMap<>();

    for (List<Object> row : rows) {
      String name = (String) row.get(0);
      String value = (String) row.get(1);

      properties.put(name, value);
    }

    return properties;
  }

  /**
   * Get table property value.
   *
   * @param db db name
   * @param table table name
   * @param name property name
   * @return property value
   * @throws RuntimeException if run error
   * @throws SQLException if execute sql error
   */
  public String getTblPropertyValue(String db, String table, String name)
      throws RuntimeException, SQLException {
    if (StringUtils.isEmpty(db) || StringUtils.isEmpty(table) || StringUtils.isEmpty(name)) {
      LOGGER.warn("db or table or name is empty, return");

      return null;
    }

    Map<String, String> properties = showTblProperties(db, table);

    if (MapUtils.isEmpty(properties)) {
      return null;
    }

    return properties.get(name);
  }

  @Override
  public void close() {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        LOGGER.error("rs close error: {}", ExceptionUtils.getFullStackTrace(e));
      }
    }

    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        LOGGER.error("stmt close error: {}", ExceptionUtils.getFullStackTrace(e));
      }
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        LOGGER.error("conn close error: {}", ExceptionUtils.getFullStackTrace(e));
      }
    }
  }
}
