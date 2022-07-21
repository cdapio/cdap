/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.explore.client.ExploreExecutionResult;
import io.cdap.cdap.explore.service.ExploreException;
import io.cdap.cdap.explore.service.MetaDataInfo;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A {@link ExploreClient} implementation that throws {@link UnsupportedOperationException} on
 * every method call. This is used in runtime environment that explore is not supported.
 */
public final class UnsupportedExploreClient implements ExploreClient {
  @Override
  public void ping() throws UnauthenticatedException, ServiceUnavailableException, ExploreException {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> enableExploreDataset(DatasetId datasetInstance,
                                                     DatasetSpecification spec, boolean truncating) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> updateExploreDataset(DatasetId datasetInstance,
                                                     DatasetSpecification oldSpec, DatasetSpecification newSpec) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> disableExploreDataset(DatasetId datasetInstance, DatasetSpecification spec) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> addPartition(DatasetId datasetInstance, DatasetSpecification spec,
                                             PartitionKey key, String path) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> dropPartition(DatasetId datasetInstance, DatasetSpecification spec, PartitionKey key) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<Void> concatenatePartition(DatasetId datasetInstance,
                                                     DatasetSpecification spec, PartitionKey key) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> submit(NamespaceId namespace, String statement) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> columns(@Nullable String catalog, @Nullable String schemaPattern,
                                                          String tableNamePattern, String columnNamePattern) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> catalogs() {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> schemas(@Nullable String catalog, @Nullable String schemaPattern) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> functions(@Nullable String catalog, @Nullable String schemaPattern,
                                                            String functionNamePattern) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<MetaDataInfo> info(MetaDataInfo.InfoType infoType) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> tables(@Nullable String catalog, @Nullable String schemaPattern,
                                                         String tableNamePattern, @Nullable List<String> tableTypes) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> tableTypes() {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> dataTypes() {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> addNamespace(NamespaceMeta namespaceMeta) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public ListenableFuture<ExploreExecutionResult> removeNamespace(NamespaceId namespace) {
    throw new UnsupportedOperationException("Explore is not supported. This method should not be called.");
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
