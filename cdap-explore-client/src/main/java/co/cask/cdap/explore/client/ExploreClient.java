/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Explore client discovers explore service, and executes explore commands using the service.
 */
public interface ExploreClient extends Closeable {

  /**
   * Pings the explore service. Returns successfully if explore service is up and running.
   *
   * @throws UnauthenticatedException if the client is not authenticated.
   * @throws ServiceUnavailableException if the explore service is not available.
   * @throws ExploreException if there is some other error when attempting to ping the explore service.
   */
  void ping() throws UnauthenticatedException, ServiceUnavailableException, ExploreException;

  /**
   * Enables ad-hoc exploration of the given dataset.
   *
   * @param datasetInstance dataset instance id
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the enable operation.
   */
  ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance);

  /**
   * Enables ad-hoc exploration of the given dataset.
   * Passing in the dataset spec avoids having to look it up in the Explore service.
   *
   * @param datasetInstance dataset instance id
   * @param spec the dataset specification of the dataset
   * @param truncating whether this call to create() is part of a truncate() operation, which is in some
   *                   case implemented using disableExplore() followed by enableExplore()
   *
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the enable operation.
   */
  ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance, DatasetSpecification spec, boolean truncating);

  /**
   * Updates ad-hoc exploration of the given dataset.
   *
   * @param datasetInstance dataset instance id
   * @param oldSpec the dataset specification from before the update
   * @param newSpec the dataset specification after the update
   * @return a {@code Future} object that can either successfully complete, or enter a failed state depending on
   *         the success of the update operation.
   */
  ListenableFuture<Void> updateExploreDataset(DatasetId datasetInstance,
                                              DatasetSpecification oldSpec,
                                              DatasetSpecification newSpec);

  /**
   * Disables ad-hoc exploration of the given dataset.
   *
   * @param datasetInstance dataset instance id
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the disable operation
   */
  ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance);

  /**
   * Disables ad-hoc exploration of the given dataset.
   * Passing in the dataset spec avoids having to look it up in the Explore service.
   *
   * @param datasetInstance dataset instance id
   * @param spec the dataset specification of the dataset
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the disable operation
   */
  ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance, DatasetSpecification spec);

  /**
   * Enables ad-hoc exploration of the given stream.
   *
   * @param stream stream id
   * @param tableName name of the Hive table to create
   * @param format format of the stream events
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the enable operation
   */
  ListenableFuture<Void> enableExploreStream(StreamId stream, String tableName, FormatSpecification format);

  /**
   * Disable ad-hoc exploration of the given stream.
   *
   * @param stream stream id
   * @param tableName the Hive table name to delete
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the enable operation
   */
  ListenableFuture<Void> disableExploreStream(StreamId stream, String tableName);

  /**
   * Add a partition to a dataset's table.
   *
   * @param datasetInstance instance of the dataset
   * @param spec the dataset specification
   * @param key the partition key
   * @param path the file system path of the partition
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the operation
   */
  ListenableFuture<Void> addPartition(DatasetId datasetInstance,
                                      DatasetSpecification spec, PartitionKey key, String path);

  /**
   * Drop a partition from a dataset's table.
   *
   * @param datasetInstance instance of the dataset
   * @param key the partition key
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the operation
   */
  ListenableFuture<Void> dropPartition(DatasetId datasetInstance,
                                       DatasetSpecification spec, PartitionKey key);

  /**
   * Concatenates the partition in the Hive table for the given dataset.
   *
   * @param datasetInstance instance of the dataset
   * @param key the partition key
   * @return a {@code Future} object that can either successfully complete, or enters a failed state, depending on
   *         the success of the operation
   */
  ListenableFuture<Void> concatenatePartition(DatasetId datasetInstance,
                                              DatasetSpecification spec, PartitionKey key);

  /**
   * Execute a Hive SQL statement asynchronously. The returned {@link ListenableFuture} can be used to get the
   * schema of the operation, and it contains an iterator on the results of the statement.
   *
   * @param namespace namespace to run the statement in
   * @param statement SQL statement
   * @return {@link ListenableFuture} eventually containing the results of the statement execution.
   */
  ListenableFuture<ExploreExecutionResult> submit(NamespaceId namespace, String statement);

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
   * @param infoType information type we are interested about
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

  /**
   * Creates a namespace in Explore.
   *
   * @param namespaceMeta namespace meta of the namespace to create.
   * @return {@link ListenableFuture} eventually creating the namespace (database in Hive).
   */
  ListenableFuture<ExploreExecutionResult> addNamespace(NamespaceMeta namespaceMeta);

  /**
   * Deletes a namespace in Explore.
   *
   * @param namespace namespace to delete.
   * @return {@link ListenableFuture} eventually deleting the namespace (database in Hive).
   */
  ListenableFuture<ExploreExecutionResult> removeNamespace(NamespaceId namespace);
}
