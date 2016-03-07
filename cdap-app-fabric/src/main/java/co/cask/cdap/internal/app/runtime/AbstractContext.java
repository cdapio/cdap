/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext extends AbstractServiceDiscoverer
  implements DatasetContext, RuntimeContext, PluginContext {

  private final Program program;
  private final RunId runId;
  private final List<Id> owners;
  private final Map<String, String> runtimeArguments;
  private final MetricsContext programMetrics;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final PluginInstantiator pluginInstantiator;
  private final PluginContext pluginContext;
  protected final DynamicDatasetCache datasetCache;

  /**
   * Constructs a context without plugin support.
   */
  protected AbstractContext(Program program, RunId runId, Arguments arguments,
                            Set<String> datasets, MetricsContext metricsContext,
                            DatasetFramework dsFramework, TransactionSystemClient txClient,
                            DiscoveryServiceClient discoveryServiceClient, boolean multiThreaded) {
    this(program, runId, arguments, datasets, metricsContext,
         dsFramework, txClient, discoveryServiceClient, multiThreaded, null);
  }

  /**
   * Constructs a context. To have plugin support, the {@code pluginInstantiator} must not be null.
   */
  protected AbstractContext(Program program, RunId runId, Arguments arguments,
                            Set<String> datasets, MetricsContext metricsContext,
                            DatasetFramework dsFramework, TransactionSystemClient txClient,
                            DiscoveryServiceClient discoveryServiceClient, boolean multiThreaded,
                            @Nullable PluginInstantiator pluginInstantiator) {
    super(program.getId());
    this.program = program;
    this.runId = runId;
    this.runtimeArguments = ImmutableMap.copyOf(arguments.asMap());
    this.discoveryServiceClient = discoveryServiceClient;
    this.owners = createOwners(program.getId());
    this.programMetrics = metricsContext;

    Map<String, Map<String, String>> staticDatasets = new HashMap<>();
    for (String name : datasets) {
      staticDatasets.put(name, runtimeArguments);
    }
    SystemDatasetInstantiator instantiator =
      new SystemDatasetInstantiator(dsFramework, program.getClassLoader(), owners);
    this.datasetCache = multiThreaded
      ? new MultiThreadDatasetCache(instantiator, txClient, new NamespaceId(namespaceId),
                                    runtimeArguments, programMetrics, staticDatasets)
      : new SingleThreadDatasetCache(instantiator, txClient, new NamespaceId(namespaceId),
                                     runtimeArguments, programMetrics, staticDatasets);
    this.pluginInstantiator = pluginInstantiator;
    this.pluginContext = new DefaultPluginContext(pluginInstantiator, program.getId(),
                                                  program.getApplicationSpecification().getPlugins());
  }

  private List<Id> createOwners(Id.Program programId) {
    ImmutableList.Builder<Id> result = ImmutableList.builder();
    result.add(programId);
    return result.build();
  }

  public List<Id> getOwners() {
    return owners;
  }

  public abstract Metrics getMetrics();

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return program.getApplicationSpecification();
  }

  @Override
  public String getNamespace() {
    return program.getNamespaceId();
  }

  /**
   * Returns the {@link PluginInstantiator} used by this context or {@code null} if there is no plugin support.
   */
  @Nullable
  public PluginInstantiator getPluginInstantiator() {
    return pluginInstantiator;
  }

  @Override
  public String toString() {
    return String.format("namespaceId=%s, applicationId=%s, program=%s, runid=%s",
                         getNamespaceId(), getApplicationId(), getProgramName(), runId);
  }

  public MetricsContext getProgramMetrics() {
    return programMetrics;
  }

  public DynamicDatasetCache getDatasetCache() {
    return datasetCache;
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return getDataset(name, RuntimeArguments.NO_ARGUMENTS);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
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

  public String getNamespaceId() {
    return program.getNamespaceId();
  }

  public String getApplicationId() {
    return program.getApplicationId();
  }

  public String getProgramName() {
    return program.getName();
  }

  public Program getProgram() {
    return program;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    datasetCache.close();
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  public static Map<String, String> getMetricsContext(Program program, String runId) {
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(ProgramTypeMetricTag.getTagName(program.getType()), program.getName());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId);
    return tags;
  }

  public abstract Map<String, Plugin> getPlugins();

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(pluginId);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return pluginContext.newPluginInstance(pluginId);
  }
}
