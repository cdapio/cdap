package com.continuuity.explore.service;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface for exploring datasets.
 */
public interface Explore {

  /**
   * Execute a Hive SQL statement asynchronously. The returned {@link Handle} can be used to get the status/result of
   * the operation.

   * @param statement SQL statement.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error executing statement.
   * @throws SQLException if there are errors in the SQL statement.
   */
  Handle execute(String statement) throws ExploreException, SQLException;

  /**
   * Fetch the status of a running Hive operation.

   * @param handle handle returned by {@link #execute(String)}.
   * @return status of the operation.
   * @throws ExploreException on any error fetching status.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  Status getStatus(Handle handle) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Fetch the schema of the result of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.OpStatus#FINISHED}.

   * @param handle handle returned by {@link #execute(String)}.
   * @return list of {@link ColumnDesc} representing the schema of the results. Empty list if there are no results.
   * @throws ExploreException on any error fetching schema.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Fetch the results of a Hive operation. This can be called only after the state of the operation is
   *               {@link Status.OpStatus#FINISHED}. Can be called multiple times, until it returns an empty list
   *               indicating the end of results.

   * @param handle handle returned by {@link #execute(String)}.
   * @param size max rows to fetch in the call.
   * @return list of {@link Result}s.
   * @throws ExploreException on any error fetching results.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  List<Result> nextResults(Handle handle, int size) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Cancel a running Hive operation. After the operation moves into a {@link Status.OpStatus#CANCELED},
   * {@link #close(Handle)} needs to be called to release resources.

   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException on any error cancelling operation.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  void cancel(Handle handle) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Release resources associated with a Hive operation. After this call, handle of the operation becomes invalid.

   * @param handle handle returned by {@link #execute(String)}.
   * @throws ExploreException on any error closing operation.
   * @throws HandleNotFoundException when handle is not found.
   */
  void close(Handle handle) throws ExploreException, HandleNotFoundException;

  ////// Metadata methods


  /**
   * Retrieves a description of table columns available in the specified catalog.
   * Only column descriptions matching the catalog, schema, table and column name criteria are returned.
   *
   * See {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database.
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the columns.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException;

  /**
   * Retrieves the catalog names available in this database.
   *
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the columns.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getCatalogs() throws ExploreException, SQLException;

  /**
   * Retrieves the schema names available in this database.
   *
   * See {@link java.sql.DatabaseMetaData#getSchemas(String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the schemas.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getSchemas(String catalog, String schemaPattern) throws ExploreException, SQLException;

  /**
   * Retrieves a description of the system and user functions available in the given catalog.
   * Only system and user function descriptions matching the schema and function name criteria are returned.
   *
   * See {@link java.sql.DatabaseMetaData#getFunctions(String, String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @param functionNamePattern a function name pattern; must match the function name as it is stored in the database
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the functions.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException;


  /**
   * Get information about Reactor as a database.
   *
   * @param infoType information type we are interested about.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the information.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException;

  /**
   * As seen in Hive code, the arguments are actually patterns
   * TODO verify those assumptions with unit tests - got that from
   * http://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTables
   *
   * Retrieves a description of the tables available in the given catalog. Only table descriptions
   * matching the catalog, schema, table name and type criteria are returned.
   *
   * See {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database.
   * @param tableTypes a list of table types, which must come from
   *                   "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM";
   *                   null returns all types.
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the tables.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getTables(String catalog, String schemaPattern, String tableNamePattern,
                          List<String> tableTypes) throws ExploreException, SQLException;

  /**
   * Retrieves the table types available in this database.
   *
   * See {@link java.sql.DatabaseMetaData#getTableTypes()}.
   *
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the table types.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getTableTypes() throws ExploreException, SQLException;

  /**
   * Retrieves a description of all the data types supported by this database.
   *
   * See {@link java.sql.DatabaseMetaData#getTableTypes()}.
   *
   * @return {@link Handle} representing the operation.
   * @throws ExploreException on any error getting the types info.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public Handle getTypeInfo() throws ExploreException, SQLException;
}
