/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.proto.TableNameInfo;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Interface for exploring datasets.
 */
public interface Explore {

  /**
   * Execute a Hive SQL statement asynchronously. The returned {@link QueryHandle} can be used to get the
   * status/result of the operation.
   *
   * @param namespace namespace to run the query in.
   * @param statement SQL statement.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error executing statement.
   * @throws SQLException if there are errors in the SQL statement.
   */
  QueryHandle execute(Id.Namespace namespace, String statement) throws ExploreException, SQLException;

  /**
   * Fetch the status of a running Hive operation.
   *
   * @param handle handle returned by {@link #execute(Id.Namespace, String)}.
   * @return status of the operation.
   * @throws ExploreException on any error fetching status.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  QueryStatus getStatus(QueryHandle handle) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Fetch the schema of the result of a Hive operation. This can be called only after the state of the operation is
   *               {@link QueryStatus.OpStatus#FINISHED}.
   *
   * @param handle handle returned by {@link #execute(Id.Namespace, String)}.
   * @return list of {@link ColumnDesc} representing the schema of the results. Empty list if there are no results.
   * @throws ExploreException on any error fetching schema.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  List<ColumnDesc> getResultSchema(QueryHandle handle) throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Fetch the results of a Hive operation. This can be called only after the state of the operation is
   * {@link QueryStatus.OpStatus#FINISHED}. Can be called multiple times, until it returns an empty list
   * indicating the end of results.
   *
   * @param handle handle returned by {@link #execute(Id.Namespace, String)}.
   * @param size max rows to fetch in the call.
   * @return list of {@link QueryResult}s.
   * @throws ExploreException on any error fetching results.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  List<QueryResult> nextResults(QueryHandle handle, int size)
    throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Fetch a preview of the results of a Hive operation. This can be called only after the state of the operation is
   * {@link QueryStatus.OpStatus#FINISHED}. Two subsequent calls to this methods will return the same list of results.
   * 
   * @param handle handle returned by {@link #execute(Id.Namespace, String)}.
   * @return preview list of {@link QueryResult}s.
   * @throws ExploreException on any error fetching a preview of the results.
   * @throws HandleNotFoundException when handle is not found.
   * @throws SQLException if there are errors in the SQL statement.
   */
  List<QueryResult> previewResults(QueryHandle handle)
    throws ExploreException, HandleNotFoundException, SQLException;

  /**
   * Release resources associated with a Hive operation. After this call, handle of the operation becomes invalid.
   *
   * @param handle handle returned by {@link #execute(Id.Namespace, String)}.
   * @throws ExploreException on any error closing operation.
   * @throws HandleNotFoundException when handle is not found.
   */
  void close(QueryHandle handle) throws ExploreException, HandleNotFoundException;

  /**
   * Fetch information about queries executed in Hive.
   *
   * @return List of {@link QueryInfo}
   * @throws ExploreException
   * @param namespace namespace to get queries in.
   */
  List<QueryInfo> getQueries(Id.Namespace namespace) throws ExploreException, SQLException;


  ////// Metadata methods

  /**
   * Retrieves a description of table columns available in the specified catalog.
   * Only column descriptions matching the catalog, schema, table and column name criteria are returned.
   *
   * See {@link DatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the database.
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in the database.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the columns.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getColumns(@Nullable String catalog, @Nullable String schemaPattern,
                                String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException;

  /**
   * Retrieves the catalog names available in this database.
   *
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the columns.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getCatalogs() throws ExploreException, SQLException;

  /**
   * Retrieves the schema names available in this database.
   *
   * See {@link DatabaseMetaData#getSchemas(String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the schemas.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getSchemas(@Nullable String catalog, @Nullable String schemaPattern)
    throws ExploreException, SQLException;

  /**
   * Retrieves a description of the system and user functions available in the given catalog.
   * Only system and user function descriptions matching the schema and function name criteria are returned.
   *
   * See {@link DatabaseMetaData#getFunctions(String, String, String)}.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database;
   *                "" retrieves those without a catalog;
   *                null means that the catalog name should not be used to narrow the search.
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the database;
   *                      "" retrieves those without a schema;
   *                      null means that the schema name should not be used to narrow the search.
   * @param functionNamePattern a function name pattern; must match the function name as it is stored in the database
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the functions.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getFunctions(@Nullable String catalog, @Nullable String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException;


  /**
   * Get information about CDAP as a database.
   *
   * @param infoType information type we are interested about.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the information.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException;

  /**
   * Retrieves a description of the tables available in the given catalog. Only table descriptions
   * matching the catalog, schema, table name and type criteria are returned.
   *
   * See {@link DatabaseMetaData#getTables(String, String, String, String[])}.
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
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the tables.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getTables(@Nullable String catalog, @Nullable String schemaPattern, String tableNamePattern,
                               @Nullable List<String> tableTypes) throws ExploreException, SQLException;

  /**
   * Retrieve a list of all the tables present in Hive Metastore that match the given database name.
   *
   * @param database database name from which to list the tables. The database has to be accessible by the current
   *                 user. If it is null, all the databases the user has access to will be inspected.
   * @return list of table names present in the database.
   * @throws ExploreException on any error getting the tables.
   */
  public List<TableNameInfo> getTables(@Nullable String database) throws ExploreException;

  /**
   * Get information about a Hive table.
   *
   * @param database name of the database the table belongs to.
   * @param table table name for which to get the schema.
   * @return information about a table.
   * @throws ExploreException on any error getting the tables.
   */
  public TableInfo getTableInfo(@Nullable String database, String table)
    throws ExploreException, TableNotFoundException;

  /**
   * Retrieves the table types available in this database.
   *
   * See {@link DatabaseMetaData#getTableTypes()}.
   *
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the table types.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getTableTypes() throws ExploreException, SQLException;

  /**
   * Retrieves a description of all the data types supported by this database.
   *
   * See {@link DatabaseMetaData#getTypeInfo()}.
   *
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any error getting the types info.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle getTypeInfo() throws ExploreException, SQLException;

  /**
   * Creates a new namespace in Explore.
   *
   * @param namespace namespace to create.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any errors creating the namespace.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle createNamespace(Id.Namespace namespace) throws ExploreException, SQLException;

  /**
   * Deletes a new namespace in Explore.
   *
   * @param namespace namespace to delete.
   * @return {@link QueryHandle} representing the operation.
   * @throws ExploreException on any errors deleting the namespace.
   * @throws SQLException if there are errors in the SQL statement.
   */
  public QueryHandle deleteNamespace(Id.Namespace namespace) throws ExploreException, SQLException;
}
