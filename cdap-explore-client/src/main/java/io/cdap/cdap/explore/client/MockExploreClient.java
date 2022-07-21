/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.explore.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.explore.service.ExploreException;
import io.cdap.cdap.explore.service.MetaDataInfo;
import io.cdap.cdap.proto.ColumnDesc;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.QueryResult;
import io.cdap.cdap.proto.QueryStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock Explore client to use in test cases.
 */
public class MockExploreClient extends AbstractIdleService implements ExploreClient {

  private final Map<String, List<ColumnDesc>> statementsToMetadata;
  private final Map<String, List<QueryResult>> statementsToResults;

  public MockExploreClient(Map<String, List<ColumnDesc>> statementsToMetadata,
                           Map<String, List<QueryResult>> statementsToResults) {
    this.statementsToMetadata = Maps.newHashMap(statementsToMetadata);
    this.statementsToResults = Maps.newHashMap(statementsToResults);
  }

  // required by Guice
  @SuppressWarnings("unused")
  public MockExploreClient() {
    this(ImmutableMap.<String, List<ColumnDesc>>of(), ImmutableMap.<String, List<QueryResult>>of());
  }

  @Override
  public void ping() throws UnauthenticatedException, ExploreException {

  }

  @Override
  public ListenableFuture<Void> updateExploreDataset(DatasetId datasetInstance,
                                                     DatasetSpecification oldSpec, DatasetSpecification newSpec) {
    return null;
  }

  @Override
  public ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance) {
    return null;
  }

  @Override
  public ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance, DatasetSpecification spec,
                                                     boolean truncating) {
    return null;
  }

  @Override
  public ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance) {
    return null;
  }

  @Override
  public ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance, DatasetSpecification spec) {
    return null;
  }

  @Override
  public ListenableFuture<Void> addPartition(DatasetId datasetInstance, DatasetSpecification spec,
                                             PartitionKey key, String path) {
    return null;
  }

  @Override
  public ListenableFuture<Void> dropPartition(DatasetId datasetInstance, DatasetSpecification spec, PartitionKey key) {
    return null;
  }

  @Override
  public ListenableFuture<Void> concatenatePartition(DatasetId datasetInstance, DatasetSpecification spec,
                                                     PartitionKey key) {
    return null;
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> submit(NamespaceId namespace, final String statement) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get(statement).iterator(),
                                                      statementsToMetadata.get(statement)));
    return new MockStatementExecutionFuture(futureDelegate, statement, statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> columns(@Nullable String catalog, @Nullable String schemaPattern,
                                                          String tableNamePattern, String columnNamePattern) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("columns_stmt").iterator(),
                                                      statementsToMetadata.get("columns_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "columns_stmt", statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> catalogs() {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("catalogs_stmt").iterator(),
                                                      statementsToMetadata.get("catalogs_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "catalogs_stmt", statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> schemas(@Nullable String catalog, @Nullable String schemaPattern) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("schemas_stmt").iterator(),
                                                      statementsToMetadata.get("schemas_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "schemas_stmt",
                                            statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> functions(@Nullable String catalog, @Nullable String schemaPattern,
                                                            String functionNamePattern) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("functions_stmt").iterator(),
                                                      statementsToMetadata.get("functions_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "functions_stmt",
                                            statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<MetaDataInfo> info(MetaDataInfo.InfoType infoType) {
    return null;
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> tables(@Nullable String catalog, @Nullable String schemaPattern,
                                                         String tableNamePattern, @Nullable List<String> tableTypes) {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("tables_stmt").iterator(),
                                                      statementsToMetadata.get("tables_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "tables_stmt",
                                            statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> tableTypes() {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("tableTypes_stmt").iterator(),
                                                      statementsToMetadata.get("tableTypes_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "tableTypes_stmt",
                                            statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> dataTypes() {
    SettableFuture<ExploreExecutionResult> futureDelegate = SettableFuture.create();
    futureDelegate.set(new MockExploreExecutionResult(statementsToResults.get("dataTypes_stmt").iterator(),
                                                      statementsToMetadata.get("dataTypes_stmt")));
    return new MockStatementExecutionFuture(futureDelegate, "dataTypes_stmt",
                                            statementsToMetadata, statementsToResults);
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> addNamespace(NamespaceMeta namespaceMeta) {
    return null;
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> removeNamespace(NamespaceId namespace) {
    return null;
  }

  @Override
  protected void startUp() throws Exception {
    // Do nothing
  }

  @Override
  protected void shutDown() throws Exception {
    // Do nothing
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }

  private static final class MockExploreExecutionResult implements ExploreExecutionResult {

    private final Iterator<QueryResult> delegate;
    private final List<ColumnDesc> schema;

    MockExploreExecutionResult(Iterator<QueryResult> delegate, List<ColumnDesc> schema) {
      this.delegate = delegate;
      this.schema = schema;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public QueryResult next() {
      return delegate.next();
    }

    @Override
    public void remove() {
      delegate.remove();
    }

    @Override
    public void close() throws IOException {
      // TODO should close the query here
    }

    @Override
    public int getFetchSize() {
      return 100;
    }

    @Override
    public void setFetchSize(int fetchSize) {
      // Do nothing
    }

    @Override
    public List<ColumnDesc> getResultSchema() throws ExploreException {
      return schema;
    }

    @Override
    public boolean canContainResults() {
      return true;
    }

    @Override
    public QueryStatus getStatus() {
      return QueryStatus.NO_OP;
    }
  }

  private static final class MockStatementExecutionFuture
    extends ForwardingListenableFuture.SimpleForwardingListenableFuture<ExploreExecutionResult>
    implements ListenableFuture<ExploreExecutionResult> {

    private final Map<String, List<ColumnDesc>> statementsToMetadata;
    private final Map<String, List<QueryResult>> statementsToResults;
    private final String statement;

    MockStatementExecutionFuture(ListenableFuture<ExploreExecutionResult> delegate, String statement,
                                 Map<String, List<ColumnDesc>> statementsToMetadata,
                                 Map<String, List<QueryResult>> statementsToResults) {
      super(delegate);
      this.statement = statement;
      this.statementsToMetadata = statementsToMetadata;
      this.statementsToResults = statementsToResults;
    }

    @Override
    public boolean cancel(boolean interrupt) {
      statementsToMetadata.remove(statement);
      statementsToResults.remove(statement);
      return true;
    }
  }
}
