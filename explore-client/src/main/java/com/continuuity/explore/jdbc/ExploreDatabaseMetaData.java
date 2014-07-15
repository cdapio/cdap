package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Explore database meta data, informing of the capabilities of Explore as a DBMS.
 */
public class ExploreDatabaseMetaData implements DatabaseMetaData {

  private static final String CATALOG_SEPARATOR = ".";

  private final ExploreClient exploreClient;

  public ExploreDatabaseMetaData(ExploreClient exploreClient) {
    this.exploreClient = exploreClient;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
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
  public String getDatabaseProductName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDriverName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDriverMajorVersion() {
    return ExploreDriver.getMajorDriverVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    return ExploreDriver.getMinorDriverVersion();
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
  public String getSQLKeywords() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
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
  public boolean supportsMultipleResultSets() throws SQLException {
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
  public boolean supportsOuterJoins() throws SQLException {
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
  public String getSchemaTerm() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
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
  public boolean supportsPositionedDelete() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
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
  public boolean supportsTransactions() throws SQLException {
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
  public ResultSet getProcedures(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getProcedureColumns(String s, String s2, String s3, String s4) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTables(String s, String s2, String s3, String[] strings) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getColumns(String s, String s2, String s3, String s4) throws SQLException {
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
  public ResultSet getPrimaryKeys(String s, String s2, String s3) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getImportedKeys(String s, String s2, String s3) throws SQLException {
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
  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSet getIndexInfo(String s, String s2, String s3, boolean b, boolean b2) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsResultSetType(int i) throws SQLException {
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
  public ResultSet getUDTs(String s, String s2, String s3, int[] ints) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
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
  public boolean supportsResultSetHoldability(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
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
  public int getSQLStateType() throws SQLException {
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
  public ResultSet getSchemas(String s, String s2) throws SQLException {
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
  public ResultSet getFunctions(String s, String s2, String s3) throws SQLException {
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
}
