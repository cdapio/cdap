package com.continuuity.explore.jdbc.meta;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExploreClientUtil;
import com.continuuity.explore.jdbc.ExploreDriver;
import com.continuuity.explore.jdbc.JdbcColumn;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;

/**
 *
 */
public class ExploreDatabaseMetaData implements java.sql.DatabaseMetaData {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDatabaseMetaData.class);

  private final ExploreClient client;
  private static final String CATALOG_SEPARATOR = ".";

  private static final char SEARCH_STRING_ESCAPE = '\\';

  //  The maximum column length = MFieldSchema.FNAME in metastore/src/model/package.jdo
  private static final int maxColumnNameLength = 128;

  /**
   *
   */
  public ExploreDatabaseMetaData(ExploreClient client) {
    this.client = client;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getAttributes(String catalog, String schemaPattern,
                                 String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema,
                                        String table, int scope, boolean nullable) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  public ResultSet getCatalogs() throws SQLException {
    try {
      // TODO a client call to get the schema's after HIVE-675 is implemented
      final List<String> catalogs = new ArrayList<String>();
      catalogs.add("default");
      return new ExploreMetaDataResultSet<String>(Arrays.asList("TABLE_CAT")
        , Arrays.asList("STRING")
        , catalogs) {
        private int cnt = 0;

        public boolean next() throws SQLException {
          if (cnt < data.size()) {
            List<Object> a = new ArrayList<Object>(1);
            a.add(data.get(cnt)); // TABLE_CAT String => table catalog (may be null)
            row = a;
            cnt++;
            return true;
          } else {
            return false;
          }
        }

        public <T> T getObject(String columnLabel, Class<T> type)
          throws SQLException {
          // JDK 1.7
          throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
        }

        public <T> T getObject(int columnIndex, Class<T> type)
          throws SQLException {
          // JDK 1.7
          throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
        }
      };
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getColumnPrivileges(String catalog, String schema,
                                       String table, String columnNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @SuppressWarnings("UnusedDeclaration")
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                    String tableNamePattern, String columnNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  @SuppressWarnings("UnusedDeclaration")
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }


  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   * these characters escaped using {@link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   * characters.
   */
  private String convertPattern(final String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      StringBuilder result = new StringBuilder(pattern.length());

      boolean escaped = false;
      for (int i = 0, len = pattern.length(); i < len; i++) {
        char c = pattern.charAt(i);
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false;
          }
          result.append(c);
        } else {
          if (c == SEARCH_STRING_ESCAPE) {
            escaped = true;
          } else if (c == '%') {
            result.append(".*");
          } else if (c == '_') {
            result.append('.');
          } else {
            result.append(Character.toLowerCase(c));
          }
        }
      }

      return result.toString();
    }
  }

  public ResultSet getColumns(String catalog, final String schemaPattern
    , final String tableNamePattern
    , final String columnNamePattern) throws SQLException {
    List<JdbcColumn> columns = new ArrayList<JdbcColumn>();
    try {
      if (catalog == null) {
        catalog = "default";
      }

      String regtableNamePattern = convertPattern(tableNamePattern);
      String regcolumnNamePattern = convertPattern(columnNamePattern);

      List<String> tables = getTableNames(catalog);
      for (String table: tables) {
        if (table.matches(regtableNamePattern)) {
          columns.addAll(describeTable(catalog, table, regcolumnNamePattern));
        }
      }
      Collections.sort(columns, new GetColumnsComparator());

      return new ExploreMetaDataResultSet<JdbcColumn>(
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"
          , "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS"
          , "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE"
          , "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION"
          , "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE"
          , "SOURCE_DATA_TYPE")
        , Arrays.asList("STRING", "STRING", "STRING", "STRING", "INT", "STRING"
        , "INT", "INT", "INT", "INT", "INT", "STRING"
        , "STRING", "INT", "INT", "INT", "INT"
        , "STRING", "STRING", "STRING", "STRING", "INT")
        , columns) {

        private int cnt = 0;

        public boolean next() throws SQLException {
          if (cnt < data.size()) {
            List<Object> a = new ArrayList<Object>(20);
            JdbcColumn column = data.get(cnt);
            a.add(column.getTableCatalog()); // TABLE_CAT String => table catalog (may be null)
            a.add(null); // TABLE_SCHEM String => table schema (may be null)
            a.add(column.getTableName()); // TABLE_NAME String => table name
            a.add(column.getColumnName()); // COLUMN_NAME String => column name
            a.add(column.getSqlType()); // DATA_TYPE short => SQL type from java.sql.Types
            a.add(column.getType()); // TYPE_NAME String => Data source dependent type name.
            a.add(column.getColumnSize()); // COLUMN_SIZE int => column size.
            a.add(null); // BUFFER_LENGTH is not used.
            a.add(column.getDecimalDigits()); // DECIMAL_DIGITS int => number of fractional digits
            a.add(column.getNumPrecRadix()); // NUM_PREC_RADIX int => typically either 10 or 2
            a.add(DatabaseMetaData.columnNullable); // NULLABLE int => is NULL allowed?
            a.add(column.getComment()); // REMARKS String => comment describing column (may be null)
            a.add(null); // COLUMN_DEF String => default value (may be null)
            a.add(null); // SQL_DATA_TYPE int => unused
            a.add(null); // SQL_DATETIME_SUB int => unused
            a.add(null); // CHAR_OCTET_LENGTH int
            a.add(column.getOrdinalPos()); // ORDINAL_POSITION int
            a.add("YES"); // IS_NULLABLE String
            a.add(null); // SCOPE_CATLOG String
            a.add(null); // SCOPE_SCHEMA String
            a.add(null); // SCOPE_TABLE String
            a.add(null); // SOURCE_DATA_TYPE short
            row = a;
            cnt++;
            return true;
          } else {
            return false;
          }
        }

        public <T> T getObject(String columnLabel, Class<T> type)
          throws SQLException {
          // JDK 1.7
          throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
        }

        public <T> T getObject(int columnIndex, Class<T> type)
          throws SQLException {
          // JDK 1.7
          throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
        }
      };
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  private List<String> getTableNames(String catalog) {
    Handle handle = null;
    try {
      handle = client.execute(String.format("SHOW TABLES IN %s", catalog));
      Status status = ExploreClientUtil.waitForCompletionStatus(client, handle, 200,
                                                                TimeUnit.MILLISECONDS, 500);
      if (status.getStatus() == Status.OpStatus.FINISHED && status.hasResults()) {
        int tabNameIndex = -1;
        List<ColumnDesc> columnDescs = client.getResultSchema(handle);
        for (ColumnDesc columnDesc : columnDescs) {
          if (columnDesc.getName().equalsIgnoreCase("tab_name")) {
            if (columnDesc.getType().equalsIgnoreCase("STRING")) {
              tabNameIndex = columnDesc.getPosition() - 1;
              break;
            }
          }
        }

        if (tabNameIndex > -1) {
          ImmutableList.Builder<String> tableNamesBuilder = ImmutableList.builder();
          List<Result> results;
          do {
            results = client.nextResults(handle, 1000);
            for (Result result : results) {
              tableNamesBuilder.add((String) result.getColumns().get(tabNameIndex));
            }
          } while (!results.isEmpty());

          return tableNamesBuilder.build();
        }
      }
      throw new RuntimeException("Failed to fetch tables for catalog " + catalog);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (handle != null) {
        try {
          client.close(handle);
        } catch (Exception e) {
          LOG.error("Got exception while closing handle {}", handle.getHandle(), e);
        }
      }
    }
  }

  private List<JdbcColumn> describeTable(String catalog, String table, String regcolumnNamePattern) {
    Handle handle = null;
    try {
      handle = client.execute(String.format("DESCRIBE %s.%s", catalog, table));
      Status status = ExploreClientUtil.waitForCompletionStatus(client, handle, 200,
                                                                TimeUnit.MILLISECONDS, 500);
      if (status.getStatus() == Status.OpStatus.FINISHED && status.hasResults()) {
        int colNameIndex = -1;
        int colTypeIndex = -1;
        int colCommentIndex = -1;
        List<ColumnDesc> columnDescs = client.getResultSchema(handle);
        for (ColumnDesc columnDesc : columnDescs) {
          if (columnDesc.getName().equalsIgnoreCase("col_name") && columnDesc.getType().equalsIgnoreCase("STRING")) {
            colNameIndex = columnDesc.getPosition() - 1;
          } else if (columnDesc.getName().equalsIgnoreCase("data_type") &&
            columnDesc.getType().equalsIgnoreCase("STRING")) {
            colTypeIndex = columnDesc.getPosition() - 1;
          } else if (columnDesc.getName().equalsIgnoreCase("comment") &&
            columnDesc.getType().equalsIgnoreCase("STRING")) {
            colCommentIndex = columnDesc.getPosition() - 1;
          }
        }

        if (colNameIndex > -1) {
          ImmutableList.Builder<JdbcColumn> tableNamesBuilder = ImmutableList.builder();
          List<Result> results;
          do {
            results = client.nextResults(handle, 1000);
            for (int i = 0; i < results.size(); i++) {
              Result result = results.get(i);
              String colName = (String) result.getColumns().get(colNameIndex);
              if (colName.matches(regcolumnNamePattern)) {
                String colType = colTypeIndex > -1 ? (String) result.getColumns().get(colTypeIndex) : "UNKNOWN";
                String colComment = colCommentIndex > -1 ? (String) result.getColumns().get(colCommentIndex) : "";
                tableNamesBuilder.add(new JdbcColumn(colName, table, catalog, colType, colComment, i));
              }
            }
          } while (!results.isEmpty());

          return tableNamesBuilder.build();
        }
      }
      throw new RuntimeException("Failed to fetch tables for catalog " + catalog);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (handle != null) {
        try {
          client.close(handle);
        } catch (Exception e) {
          LOG.error("Got exception while closing handle {}", handle.getHandle(), e);
        }
      }
    }
  }

  /**
   * We sort the output of getColumns to guarantee jdbc compliance.
   * First check by table name then by ordinal position
   */
  private class GetColumnsComparator implements Comparator<JdbcColumn> {

    public int compare(JdbcColumn o1, JdbcColumn o2) {
      int compareName = o1.getTableName().compareTo(o2.getTableName());
      if (compareName == 0) {
        if (o1.getOrdinalPos() > o2.getOrdinalPos()) {
          return 1;
        } else if (o1.getOrdinalPos() < o2.getOrdinalPos()) {
          return -1;
        }
        return 0;
      } else {
        return compareName;
      }
    }
  }

  public Connection getConnection() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getCrossReference(String primaryCatalog,
                                     String primarySchema, String primaryTable, String foreignCatalog,
                                     String foreignSchema, String foreignTable) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getDatabaseMajorVersion() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getDatabaseMinorVersion() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getDatabaseProductName() throws SQLException {
    return "Hive";
  }

  public String getDatabaseProductVersion() throws SQLException {
    // TODO: implement version
    return "REACTOR_JDBC_2.3";
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public int getDriverMajorVersion() {
    return ExploreDriver.getMajorDriverVersion();
  }

  public int getDriverMinorVersion() {
    return ExploreDriver.getMinorDriverVersion();
  }

  public String getDriverName() throws SQLException {
    // TODO: implement driver name
    String name = ExploreDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_TITLE);
    return name == null ? "ReactorJDBCDriver" : name;
  }

  public String getDriverVersion() throws SQLException {
    // TODO: implement driver version
    String version = ExploreDriver.fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
    return version == null ? "0.1" : version;
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getExtraNameCharacters() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
                                      String arg3) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getFunctions(String arg0, String arg1, String arg2)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getIdentifierQuoteString() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table,
                                boolean unique, boolean approximate) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  /**
   *  Returns the value of maxColumnNameLength.
   *
   */
  public int getMaxColumnNameLength() throws SQLException {
    return maxColumnNameLength;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxConnections() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxIndexLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxRowSize() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxStatementLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxStatements() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxTableNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getMaxUserNameLength() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getNumericFunctions() throws SQLException {
    return "";
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                       String procedureNamePattern, String columnNamePattern)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getProcedureTerm() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getProcedures(String catalog, String schemaPattern,
                                 String procedureNamePattern) throws SQLException {
    return null;
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getSQLKeywords() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  public String getSchemaTerm() throws SQLException {
    return "";
  }

  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  public ResultSet getSchemas(String catalog, String schemaPattern)
    throws SQLException {
    return new ExploreMetaDataResultSet<Object>(Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG")
      , Arrays.asList("STRING", "STRING"), null) {

      public boolean next() throws SQLException {
        return false;
      }

      public <T> T getObject(String columnLabel, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }

      public <T> T getObject(int columnIndex, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }
    };

  }

  public String getSearchStringEscape() throws SQLException {
    return String.valueOf(SEARCH_STRING_ESCAPE);
  }

  public String getStringFunctions() throws SQLException {
    return "";
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern,
                                  String tableNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                 String typeNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getSystemFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                      String tableNamePattern) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getTableTypes() throws SQLException {
    final TableType[] tt = TableType.values();
    return new ExploreMetaDataResultSet<TableType>(
      Arrays.asList("TABLE_TYPE")
      , Arrays.asList("STRING"), new ArrayList<TableType>(Arrays.asList(tt))) {
      private int cnt = 0;

      public boolean next() throws SQLException {
        if (cnt < data.size()) {
          List<Object> a = new ArrayList<Object>(1);
          a.add(toJdbcTableType(data.get(cnt).name()));
          row = a;
          cnt++;
          return true;
        } else {
          return false;
        }
      }

      public <T> T getObject(String columnLabel, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }

      public <T> T getObject(int columnIndex, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }
    };
  }

  public ResultSet getTables(String catalog, String schemaPattern,
                             String tableNamePattern, String[] types) throws SQLException {
    final List<String> tablesstr;
    final List<JdbcTable> resultTables = new ArrayList<JdbcTable>();
    final String resultCatalog;
    if (catalog == null) { // On jdbc the default catalog is null but on hive it's "default"
      resultCatalog = "default";
    } else {
      resultCatalog = catalog;
    }

    boolean getTables = false;
    if (types != null) {
      for (String type : types) {
        if (type.equalsIgnoreCase("TABLE")) {
          getTables = true;
        }
      }
    } else {
      getTables = true;
    }

    if (getTables) {
      String regtableNamePattern = convertPattern(tableNamePattern);
      try {
        tablesstr = getTableNames(resultCatalog);
        for (String tablestr: tablesstr) {
          if (tablestr.matches(regtableNamePattern)) {
            // TODO: handle managed tables v/s external tables
            resultTables.add(new JdbcTable(resultCatalog, tablestr, TableType.MANAGED_TABLE.toString(), ""));
          }
        }
        Collections.sort(resultTables, new GetTablesComparator());
      } catch (Exception e) {
        throw new SQLException(e);
      }
    }

    return new ExploreMetaDataResultSet<JdbcTable>(
      Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
      , Arrays.asList("STRING", "STRING", "STRING", "STRING", "STRING")
      , resultTables) {
      private int cnt = 0;

      public boolean next() throws SQLException {
        if (cnt < data.size()) {
          List<Object> a = new ArrayList<Object>(5);
          JdbcTable table = data.get(cnt);
          a.add(table.getTableCatalog()); // TABLE_CAT String => table catalog (may be null)
          a.add(null); // TABLE_SCHEM String => table schema (may be null)
          a.add(table.getTableName()); // TABLE_NAME String => table name
          try {
            a.add(table.getSqlTableType()); // TABLE_TYPE String => "TABLE","VIEW"
          } catch (Exception e) {
            throw new SQLException(e);
          }
          a.add(table.getComment()); // REMARKS String => explanatory comment on the table
          row = a;
          cnt++;
          return true;
        } else {
          return false;
        }
      }

      public <T> T getObject(String columnLabel, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }

      public <T> T getObject(int columnIndex, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }

    };
  }

  /**
   * We sort the output of getTables to guarantee jdbc compliance.
   * First check by table type then by table name
   */
  private class GetTablesComparator implements Comparator<JdbcTable> {

    public int compare(JdbcTable o1, JdbcTable o2) {
      int compareType = o1.getType().compareTo(o2.getType());
      if (compareType == 0) {
        return o1.getTableName().compareTo(o2.getTableName());
      } else {
        return compareType;
      }
    }
  }

  /**
   * Translate hive table types into jdbc table types.
   * @param hivetabletype
   * @return the type of the table
   */
  public static String toJdbcTableType(String hivetabletype) {
    if (hivetabletype == null) {
      return null;
    } else if (hivetabletype.equals(TableType.MANAGED_TABLE.toString())) {
      return "TABLE";
    } else if (hivetabletype.equals(TableType.VIRTUAL_VIEW.toString())) {
      return "VIEW";
    } else if (hivetabletype.equals(TableType.EXTERNAL_TABLE.toString())) {
      return "EXTERNAL TABLE";
    } else {
      return hivetabletype;
    }
  }

  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getUDTs(String catalog, String schemaPattern,
                           String typeNamePattern, int[] types) throws SQLException {

    return new ExploreMetaDataResultSet<Object>(
      Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE"
        , "REMARKS", "BASE_TYPE")
      , Arrays.asList("STRING", "STRING", "STRING", "STRING", "INT", "STRING", "INT")
      , null) {

      public boolean next() throws SQLException {
        return false;
      }

      public <T> T getObject(String columnLabel, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }

      public <T> T getObject(int columnIndex, Class<T> type)
        throws SQLException {
        // JDK 1.7
        throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
      }
    };
  }

  public String getURL() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public String getUserName() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isReadOnly() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsResultSetHoldability(int holdability)
    throws SQLException {
    return false;
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsTransactionIsolationLevel(int level)
    throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean supportsUnionAll() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean usesLocalFiles() throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Method not supported ExploreDatabaseMetaData " + 
      new Exception().getStackTrace()[0].getLineNumber());
  }

  public static void main(String[] args) throws SQLException {
    ExploreDatabaseMetaData meta = new ExploreDatabaseMetaData(null);
    System.out.println("DriverName: " + meta.getDriverName());
    System.out.println("DriverVersion: " + meta.getDriverVersion());
  }
}
