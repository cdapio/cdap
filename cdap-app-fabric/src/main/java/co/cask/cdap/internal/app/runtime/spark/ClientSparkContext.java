/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkContext} implementation that is used in {@link Spark#beforeSubmit(SparkContext)} and
 * {@link Spark#onFinish(boolean, SparkContext)}.
 */
public final class ClientSparkContext extends AbstractSparkContext {

  private final TransactionContext transactionContext;
  private final DynamicDatasetCache datasetCache;
  private final File pluginArchive;
  private final Map<String, LocalizeResource> resourcesToLocalize;

  public ClientSparkContext(Program program, RunId runId, long logicalStartTime, Map<String, String> runtimeArguments,
                            TransactionSystemClient txClient, DatasetFramework datasetFramework,
                            DiscoveryServiceClient discoveryServiceClient,
                            MetricsCollectionService metricsCollectionService,
                            @Nullable File pluginArchive,
                            @Nullable PluginInstantiator pluginInstantiator,
                            @Nullable WorkflowToken workflowToken) {
    super(program.getApplicationSpecification(),
          program.getApplicationSpecification().getSpark().get(program.getName()),
          program.getId(), runId, program.getClassLoader(), logicalStartTime,
          runtimeArguments, discoveryServiceClient,
          createMetricsContext(metricsCollectionService, program.getId(), runId),
          createLoggingContext(program.getId(), runId), pluginInstantiator, workflowToken);

    this.datasetCache = new SingleThreadDatasetCache(
      new SystemDatasetInstantiator(datasetFramework, program.getClassLoader(), getOwners()),
      txClient, new NamespaceId(program.getId().getNamespace().getId()), runtimeArguments, getMetricsContext(), null);
    this.transactionContext = datasetCache.newTransactionContext();
    this.pluginArchive = pluginArchive;
    this.resourcesToLocalize = new HashMap<>();
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass, Map<String, String> datasetArgs) {
    throw new UnsupportedOperationException("Only supported in SparkProgram.run() execution context");
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass,
                                 Map<String, String> datasetArgs) {
    throw new UnsupportedOperationException("Only supported in SparkProgram.run() execution context");
  }

  @Override
  public <T> T readFromStream(StreamBatchReadable stream, Class<?> vClass) {
    throw new UnsupportedOperationException("Only supported in SparkProgram.run() execution context");
  }

  @Override
  public <T> T getOriginalSparkContext() {
    throw new UnsupportedOperationException("Only supported in SparkProgram.run() execution context");
  }

  @Override
  public TaskLocalizationContext getTaskLocalizationContext() {
    throw new UnsupportedOperationException("Only supported in SparkProgram.run() execution context");
  }

  @Override
  public synchronized <T extends Dataset> T getDataset(String name, Map<String, String> arguments) {
    return datasetCache.getDataset(name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    datasetCache.releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    datasetCache.discardDataset(dataset);
  }

  @Override
  public synchronized void close() {
    datasetCache.close();
  }

  /**
   * Returns the {@link TransactionContext} for this context.
   */
  public TransactionContext getTransactionContext() {
    return transactionContext;
  }

  @Nullable
  public File getPluginArchive() {
    return pluginArchive;
  }

  @Override
  public void localize(String name, URI uri) {
    localize(name, uri, false);
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    URI actualURI;
    try {
      actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), name);
    } catch (URISyntaxException e) {
      // Most of the URI is constructed from the passed URI. So ideally, this should not happen.
      // If it does though, there is nothing that clients can do to recover, so not propagating a checked exception.
      throw Throwables.propagate(e);
    }
    LocalizeResource localizeResource = new LocalizeResource(actualURI, archive);
    resourcesToLocalize.put(name, localizeResource);
  }

  /**
   * Returns the localized resources for the spark program.
   */
  Map<String, LocalizeResource> getResourcesToLocalize() {
    return resourcesToLocalize;
  }
}
