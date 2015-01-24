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

package co.cask.cdap.explore.client;

import co.cask.cdap.explore.service.MetaDataInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Explore client discovers explore service, and executes explore commands using the service.
 */
public interface ExploreClient extends Closeable {

  /**
   * Returns true if the explore service is up and running.
   */
  boolean isServiceAvailable();

  /**
   * Enables ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance name.
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the enable operation.
   */
  ListenableFuture<Void> enableExploreDataset(String datasetInstance);

  /**
   * Disable ad-hoc exploration of the given {@link co.cask.cdap.api.data.batch.RecordScannable}.
   *
   * @param datasetInstance dataset instance name.
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the disable operation.
   */
  ListenableFuture<Void> disableExploreDataset(String datasetInstance);

  /**
   * Enables ad-hoc exploration of the given stream.
   *
   * @param streamName stream name.
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the enable operation.
   */
  ListenableFuture<Void> enableExploreStream(String streamName);

  /**
   * Disable ad-hoc exploration of the given stream.
   *
   * @param streamName stream name.
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the enable operation.
   */
  ListenableFuture<Void> disableExploreStream(String streamName);

  /**
   * Add a partition to a dataset's table.
   *
   * @param datasetName name of the dataset
   * @param time the partition time
   * @param path the file system path of the partition
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the operation.
   */
  ListenableFuture<Void> addPartition(String datasetName, long time, String path);

  /**
   * Drop a partition from a dataset's table.
   *
   * @param datasetName name of the dataset
   * @param time the partition time
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the operation.
   */
  ListenableFuture<Void> dropPartition(String datasetName, long time);

  /**
   * Execute a Hive SQL statement asynchronously. The returned {@link ListenableFuture} can be used to get the
   * schema of the operation, and it contains an iterator on the results of the statement.
   *
   * @param statement SQL statement.
   * @return {@link ListenableFuture} eventually containing the results of the statement execution.
   */
  ListenableFuture<ExploreExecutionResult> submit(String statement);

  ///// METADATA

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
   * @return {@link ListenableFuture} eventually containing the columns of interest.
   */
  ListenableFuture<ExploreExecutionResult> columns(@Nullable String catalog, @Nullable String schemaPattern,
                                                   String tableNamePattern, String columnNamePattern);

  /**
   * Retrieves the catalog names available in this database.
   *
   * @return {@link ListenableFuture} eventually containing the catalogs.
   */
  ListenableFuture<ExploreExecutionResult> catalogs();

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
   * @return {@link ListenableFuture} eventually containing the schemas of interest.
   */
  ListenableFuture<ExploreExecutionResult> schemas(@Nullable String catalog, @Nullable String schemaPattern);

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
   * @return {@link ListenableFuture} eventually containing the functions of interest.
   */
  ListenableFuture<ExploreExecutionResult> functions(@Nullable String catalog, @Nullable String schemaPattern,
                                                     String functionNamePattern);


  /**
   * Get information about CDAP as a database.
   *
   * @param infoType information type we are interested about.
   * @return a {@link ListenableFuture} object eventually containing the information requested.
   */
  ListenableFuture<MetaDataInfo> info(MetaDataInfo.InfoType infoType);

  /**
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
   * @return {@link ListenableFuture} eventually containing the tables of interest.
   */
  ListenableFuture<ExploreExecutionResult> tables(@Nullable String catalog, @Nullable String schemaPattern,
                                                  String tableNamePattern, @Nullable List<String> tableTypes);

  /**
   * Retrieves the table types available in this database.
   *
   * See {@link java.sql.DatabaseMetaData#getTableTypes()}.
   *
   * @return {@link ListenableFuture} eventually containing the different table types available in Explore.
   */
  ListenableFuture<ExploreExecutionResult> tableTypes();

  /**
   * Retrieves a description of all the data types supported by this database.
   *
   * See {@link java.sql.DatabaseMetaData#getTypeInfo()}.
   *
   * @return {@link ListenableFuture} eventually containing the different data types available in Explore.
   */
  ListenableFuture<ExploreExecutionResult> dataTypes();
}
