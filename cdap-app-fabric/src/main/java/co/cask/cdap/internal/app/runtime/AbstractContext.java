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

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.app.metrics.ProgramUserMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.services.AbstractServiceDiscoverer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.dataset2.SingleThreadDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.Maps;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Base class for program runtime context
 */
public abstract class AbstractContext extends AbstractServiceDiscoverer
  implements SecureStore, DatasetContext, RuntimeContext, PluginContext {

  private final Program program;
  private final ProgramOptions programOptions;
  private final RunId runId;
  private final Iterable<? extends Id> owners;
  private final Map<String, String> runtimeArguments;
  private final Metrics userMetrics;
  private final MetricsContext programMetrics;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final PluginInstantiator pluginInstantiator;
  private final PluginContext pluginContext;
  private final Admin admin;
  private final long logicalStartTime;
  private final SecureStore secureStore;
  protected final DynamicDatasetCache datasetCache;

  /**
   * Constructs a context without plugin support.
   */
  protected AbstractContext(Program program, ProgramOptions programOptions,
                            Set<String> datasets, DatasetFramework dsFramework, TransactionSystemClient txClient,
                            DiscoveryServiceClient discoveryServiceClient, boolean multiThreaded,
                            @Nullable MetricsCollectionService metricsService, Map<String, String> metricsTags,
                            SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this(program, programOptions, datasets, dsFramework, txClient,
         discoveryServiceClient, multiThreaded, metricsService, metricsTags, secureStore, secureStoreManager, null);
  }

  /**
   * Constructs a context. To have plugin support, the {@code pluginInstantiator} must not be null.
   */
  protected AbstractContext(Program program, ProgramOptions programOptions,
                            Set<String> datasets, DatasetFramework dsFramework, TransactionSystemClient txClient,
                            DiscoveryServiceClient discoveryServiceClient, boolean multiThreaded,
                            @Nullable MetricsCollectionService metricsService, Map<String, String> metricsTags,
                            SecureStore secureStore, SecureStoreManager secureStoreManager,
                            @Nullable PluginInstantiator pluginInstantiator) {
    super(program.getId().toEntityId());

    this.program = program;
    this.programOptions = programOptions;
    this.runId = ProgramRunners.getRunId(programOptions);
    this.discoveryServiceClient = discoveryServiceClient;
    this.owners = createOwners(program.getId());
    this.programMetrics = createProgramMetrics(program, runId, metricsService, metricsTags);
    this.userMetrics = new ProgramUserMetrics(programMetrics);

    Map<String, String> runtimeArgs = new HashMap<>(programOptions.getUserArguments().asMap());
    this.logicalStartTime = ProgramRunners.updateLogicalStartTime(runtimeArgs);
    this.runtimeArguments = Collections.unmodifiableMap(runtimeArgs);

    Map<String, Map<String, String>> staticDatasets = new HashMap<>();
    for (String name : datasets) {
      staticDatasets.put(name, runtimeArguments);
    }
    SystemDatasetInstantiator instantiator =
      new SystemDatasetInstantiator(dsFramework, program.getClassLoader(), owners);
    this.datasetCache = multiThreaded
      ? new MultiThreadDatasetCache(instantiator, txClient, program.getId().getNamespace().toEntityId(),
                                    runtimeArguments, programMetrics, staticDatasets)
      : new SingleThreadDatasetCache(instantiator, txClient, program.getId().getNamespace().toEntityId(),
                                     runtimeArguments, programMetrics, staticDatasets);
    this.pluginInstantiator = pluginInstantiator;
    this.pluginContext = new DefaultPluginContext(pluginInstantiator, program.getId(),
                                                  program.getApplicationSpecification().getPlugins());
    this.admin = new DefaultAdmin(dsFramework, program.getId().getNamespace().toEntityId(), secureStoreManager);
    this.secureStore = secureStore;
  }

  private Iterable<? extends Id> createOwners(Id.Program programId) {
    return Collections.singletonList(programId);
  }

  /**
   * Creates a {@link MetricsContext} for metrics emission of the program represented by this context.
   *
   * @param program the {@link Program} context that the metrics should be emitted as
   * @param runId the {@link RunId} of the current execution
   * @param metricsService the underlying service for metrics publishing; or {@code null} to suppress metrics publishing
   * @param metricsTags a set of extra tags to be used for creating the {@link MetricsContext}
   * @return a {@link MetricsContext} for emitting metrics for the current program context.
   */
  private MetricsContext createProgramMetrics(Program program, RunId runId,
                                              @Nullable MetricsCollectionService metricsService,
                                              Map<String, String> metricsTags) {

    Map<String, String> tags = Maps.newHashMap(metricsTags);
    tags.put(Constants.Metrics.Tag.NAMESPACE, program.getNamespaceId());
    tags.put(Constants.Metrics.Tag.APP, program.getApplicationId());
    tags.put(ProgramTypeMetricTag.getTagName(program.getType()), program.getName());
    tags.put(Constants.Metrics.Tag.RUN_ID, runId.getId());

    return metricsService == null ? new NoopMetricsContext(tags) : metricsService.getContext(tags);
  }

  /**
   * Returns a list of ID who owns this program context.
   */
  public Iterable<? extends Id> getOwners() {
    return owners;
  }

  /**
   * Returns a {@link Metrics} to be used inside user program.
   */
  public Metrics getMetrics() {
    return userMetrics;
  }

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
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    return getDataset(namespace, name, RuntimeArguments.NO_ARGUMENTS);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(name, arguments, AccessType.UNKNOWN);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
    throws DatasetInstantiationException {
    return getDataset(namespace, name, arguments, AccessType.UNKNOWN);
  }

  protected <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments,
                                             AccessType accessType) throws DatasetInstantiationException {
    return datasetCache.getDataset(namespace, name, arguments, accessType);
  }

  protected <T extends Dataset> T getDataset(String name, Map<String, String> arguments, AccessType accessType)
    throws DatasetInstantiationException {
    return datasetCache.getDataset(name, arguments, accessType);
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

  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  /**
   * Release all resources held by this context, for example, datasets. Subclasses should override this
   * method to release additional resources.
   */
  public void close() {
    datasetCache.close();
  }

  /**
   * Returns the {@link ProgramOptions} for the program execution that this context represents.
   */
  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

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

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InstantiationException {
    return pluginContext.newPluginInstance(pluginId, evaluator);
  }

  @Override
  public Admin getAdmin() {
    return admin;
  }

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) throws IOException {
    return secureStore.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws IOException {
    return secureStore.getSecureData(namespace, name);
  }

}
