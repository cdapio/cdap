/*
 * Copyright © 2014-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.MetaDataInfo;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Explore database meta data, informing of the capabilities of Explore as a DBMS.
 */
public class ExploreDatabaseMetaData implements DatabaseMetaData {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDatabaseMetaData.class);

  private static final int RESULT_FETCH_SIZE = 100;
  private static final String CATALOG_SEPARATOR = ".";
  private static final char SEARCH_STRING_ESCAPE = '\\';

  private static final String DRIVER_NAME = "CDAP JDBC";

  private final ExploreConnection connection;
  private final ExploreClient exploreClient;

  //  Cached values, to save on round trips to database.
  private String dbVersion = null;

  public ExploreDatabaseMetaData(ExploreConnection connection, ExploreClient exploreClient) {
    this.connection = connection;
    this.exploreClient = exploreClient;
  }

  private ResultSet getMetadataResultSet(ListenableFuture<ExploreExecutionResult> future) throws SQLException {
    try {
      return new ExploreResultSet(future.get(), RESULT_FETCH_SIZE);
    } catch (InterruptedException e) {
      LOG.error("Caught exception", e);
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    } catch (ExecutionException e) {
      LOG.error("Error executing query", e);
      throw new SQLException(e);
    } catch (CancellationException e) {
      // If futureResults has been cancelled
      LOG.error("Execution has been cancelled.", e);
      throw new SQLException(e);
    }
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return getMetadataResultSet(exploreClient.tableTypes());
  }

  @Override
  public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                              final String columnNamePattern) throws SQLException {
    return getMetadataResultSet(exploreClient.columns(catalog, schemaPattern, tableNamePattern, columnNamePattern));
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return getMetadataResultSet(exploreClient.dataTypes());
  }

  @Override
  public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                             final String[] types) throws SQLException {
    return getMetadataResultSet(exploreClient.tables(catalog, schemaPattern, tableNamePattern,
                                                     (types == null) ? null : Arrays.asList(types)));
  }

  @Override
  public ResultSet getFunctions(final String catalog, final String schemaPattern,
                                final String functionNamePattern) throws SQLException {
    return getMetadataResultSet(exploreClient.functions(catalog, schemaPattern, functionNamePattern));
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
    return getMetadataResultSet(exploreClient.schemas(catalog, schemaPattern));
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    return getMetadataResultSet(exploreClient.catalogs());
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public String getDriverName() throws SQLException {
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return String.format("%d.%d.%d", ExploreDriver.getMajorDriverVersion(), ExploreDriver.getMinorDriverVersion(),
                         ExploreDriver.getFixDriverVersion());
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return getInfo(MetaDataInfo.InfoType.DBMS_NAME).getStringValue();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    if (dbVersion == null) {
      dbVersion = getInfo(MetaDataInfo.InfoType.DBMS_VER).getStringValue();
    }
    return dbVersion;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return ExploreJDBCUtils.getVersionPart(getDatabaseProductVersion(), 0);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return ExploreJDBCUtils.getVersionPart(getDatabaseProductVersion(), 1);
  }

  @Override
  public int getDriverMajorVersion() {
    return ExploreDriver.getMajorDriverVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    return ExploreDriver.getMinorDriverVersion();
  }

  private MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws SQLException {
    try {
      return exploreClient.info(infoType).get();
    } catch (Exception e) {
      LOG.error("Error while retrieving information {}.", infoType.name(), e);
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    // NOTE: This static block is copied from Hive JDBC Driver
    return new StaticEmptyExploreResultSet(
      ImmutableList.of(
        ImmutablePair.of("PKTABLE_CAT", "STRING"),
        ImmutablePair.of("PKTABLE_SCHEM", "STRING"),
        ImmutablePair.of("PKTABLE_NAME", "STRING"),
        ImmutablePair.of("PKCOLUMN_NAME", "STRING"),
        ImmutablePair.of("FKTABLE_CAT", "STRING"),
        ImmutablePair.of("FKTABLE_SCHEM", "STRING"),
        ImmutablePair.of("FKTABLE_NAME", "STRING"),
        ImmutablePair.of("FKCOLUMN_NAME", "STRING"),
        ImmutablePair.of("KEY_SEQ", "SMALLINT"),
        ImmutablePair.of("UPDATE_RULE", "SMALLINT"),
        ImmutablePair.of("DELETE_RULE", "SMALLINT"),
        ImmutablePair.of("FK_NAME", "STRING"),
        ImmutablePair.of("PK_NAME", "STRING"),
        ImmutablePair.of("DEFERRABILITY", "STRING")
      )
    );
  }

  @Override
  public ResultSet getPrimaryKeys(String s, String s2, String s3) throws SQLException {
    // NOTE: This static block is copied from Hive JDBC Driver
    return new StaticEmptyExploreResultSet(
      ImmutableList.of(
        ImmutablePair.of("TABLE_CAT", "STRING"),
        ImmutablePair.of("TABLE_SCHEM", "STRING"),
        ImmutablePair.of("TABLE_NAME", "STRING"),
        ImmutablePair.of("COLUMN_NAME", "STRING"),
        ImmutablePair.of("KEY_SEQ", "INT"),
        ImmutablePair.of("PK_NAME", "STRING")
      )
    );
  }

  @Override
  public ResultSet getProcedureColumns(String s, String s2, String s3, String s4) throws SQLException {
    // NOTE: This static block is copied from Hive JDBC Driver
    return new StaticEmptyExploreResultSet(
      ImmutableList.of(
        ImmutablePair.of("PROCEDURE_CAT", "STRING"),
        ImmutablePair.of("PROCEDURE_SCHEM", "STRING"),
        ImmutablePair.of("PROCEDURE_NAME", "STRING"),
        ImmutablePair.of("COLUMN_NAME", "STRING"),
        ImmutablePair.of("COLUMN_TYPE", "SMALLINT"),
        ImmutablePair.of("DATA_TYPE", "INT"),
        ImmutablePair.of("TYPE_NAME", "STRING"),
        ImmutablePair.of("PRECISION", "INT"),
        ImmutablePair.of("LENGTH", "INT"),
        ImmutablePair.of("SCALE", "SMALLINT"),
        ImmutablePair.of("RADIX", "SMALLINT"),
        ImmutablePair.of("NULLABLE", "SMALLINT"),
        ImmutablePair.of("REMARKS", "STRING"),
        ImmutablePair.of("COLUMN_DEF", "STRING"),
        ImmutablePair.of("SQL_DATA_TYPE", "INT"),
        ImmutablePair.of("SQL_DATETIME_SUB", "INT"),
        ImmutablePair.of("CHAR_OCTET_LENGTH", "INT"),
        ImmutablePair.of("ORDINAL_POSITION", "INT"),
        ImmutablePair.of("IS_NULLABLE", "STRING"),
        ImmutablePair.of("SPECIFIC_NAME", "STRING")
      )
    );
  }

  @Override
  public ResultSet getProcedures(String s, String s2, String s3) throws SQLException {
    // NOTE: This static block is copied from Hive JDBC Driver
    return new StaticEmptyExploreResultSet(
      ImmutableList.of(
        ImmutablePair.of("PROCEDURE_CAT", "STRING"),
        ImmutablePair.of("PROCEDURE_SCHEM", "STRING"),
        ImmutablePair.of("PROCEDURE_NAME", "STRING"),
        ImmutablePair.of("RESERVERD", "STRING"),
        ImmutablePair.of("RESERVERD", "INT"),
        ImmutablePair.of("RESERVERD", "STRING"),
        ImmutablePair.of("REMARKS", "STRING"),
        ImmutablePair.of("PROCEDURE_TYPE", "SMALLINT"),
        ImmutablePair.of("SPECIFIC_NAME", "STRING")
      )
    );
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    // NOTE: this is what Hive returns in its JDBC Driver
    return "";
  }

  @Override
  public int getSQLStateType() throws SQLException {
    // NOTE: this is what Hive returns in its JDBC Driver
    return DatabaseMetaData.sqlStateSQL99;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "database";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return String.valueOf(SEARCH_STRING_ESCAPE);
  }

  @Override
  public String getStringFunctions() throws SQLException {
    // NOTE: this is what Hive returns in its JDBC Driver
    return "";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    // NOTE: this is what Hive returns in its JDBC Driver
    return "";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    // NOTE: this is what Hive returns in its JDBC Driver
    return "";
  }

  @Override
  public ResultSet getUDTs(String s, String s2, String s3, int[] ints) throws SQLException {
    // NOTE: This static block is copied from Hive JDBC Driver
    return new StaticEmptyExploreResultSet(
      ImmutableList.of(
        ImmutablePair.of("TYPE_CAT", "STRING"),
        ImmutablePair.of("TYPE_SCHEM", "STRING"),
        ImmutablePair.of("TYPE_NAME", "STRING"),
        ImmutablePair.of("CLASS_NAME", "STRING"),
        ImmutablePair.of("DATA_TYPE", "INT"),
        ImmutablePair.of("REMARKS", "STRING"),
        ImmutablePair.of("BASE_TYPE", "INT")
      )
    );
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;  // So Hive says
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetType(int i) throws SQLException {
    return true;
  }

  @Override
  public boolean supportsResultSetHoldability(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return "";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    // NOTE: this is Hive's preferred term for 'Procedure'
    return "UDF";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    // NOTE: this is Hive's preferred term for 'Catalog'
    return "instance";
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getURL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getUserName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsConvert(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getColumnPrivileges(String s, String s2, String s3, String s4) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTablePrivileges(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getBestRowIdentifier(String s, String s2, String s3, int i, boolean b) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getVersionColumns(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getExportedKeys(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getCrossReference(String s, String s2, String s3, String s4, String s5, String s6)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getIndexInfo(String s, String s2, String s3, boolean b, boolean b2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetConcurrency(int i, int i2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownUpdatesAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownDeletesAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean ownInsertsAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersUpdatesAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersDeletesAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean othersInsertsAreVisible(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean updatesAreDetected(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean deletesAreDetected(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean insertsAreDetected(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSuperTypes(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSuperTables(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getAttributes(String s, String s2, String s3, String s4) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getFunctionColumns(String s, String s2, String s3, String s4) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                    String tableNamePattern, String columnNamePattern) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }
}
