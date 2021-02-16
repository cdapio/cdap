/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.spark.SparkClientContext;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import org.apache.spark.SparkConf;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkClientContext} only being used by a Spark program's {@link ProgramLifecycle#initialize}.
 */
final class BasicSparkClientContext implements SparkClientContext {

  private final SparkRuntimeContext sparkRuntimeContext;
  private final Map<String, LocalizeResource> localizeResources;
  private final List<URI> additionalPythonLocations;
  private Resources driverResources;
  private Resources executorResources;
  private SparkConf sparkConf;
  private ProgramState state;
  private String pySparkScript;
  private URI pySparkScriptLocation;

  BasicSparkClientContext(SparkRuntimeContext sparkRuntimeContext) {
    this.sparkRuntimeContext = sparkRuntimeContext;
    this.localizeResources = new HashMap<>();
    this.additionalPythonLocations = new LinkedList<>();

    SparkSpecification spec = sparkRuntimeContext.getSparkSpecification();
    this.driverResources = SystemArguments.getResources(getDriverRuntimeArguments(), spec.getDriverResources());
    this.executorResources = SystemArguments.getResources(getExecutorRuntimeArguments(), spec.getExecutorResources());
  }

  @Override
  public SparkSpecification getSpecification() {
    return sparkRuntimeContext.getSparkSpecification();
  }

  @Override
  public long getLogicalStartTime() {
    return sparkRuntimeContext.getLogicalStartTime();
  }

  @Override
  public Metrics getMetrics() {
    return sparkRuntimeContext;
  }

  @Override
  public void setDriverResources(Resources resources) {
    this.driverResources = resources;
  }

  @Override
  public void setExecutorResources(Resources resources) {
    this.executorResources = resources;
  }

  @Override
  public <T> void setSparkConf(T sparkConf) {
    this.sparkConf = (SparkConf) sparkConf;
  }

  @Nullable
  @Override
  public WorkflowToken getWorkflowToken() {
    WorkflowProgramInfo workflowProgramInfo = sparkRuntimeContext.getWorkflowInfo();
    return workflowProgramInfo == null ? null : workflowProgramInfo.getWorkflowToken();
  }

  @Nullable
  @Override
  public WorkflowInfo getWorkflowInfo() {
    return sparkRuntimeContext.getWorkflowInfo();
  }

  @Override
  public void localize(String name, URI uri) {
    localize(name, uri, false);
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    try {
      URI actualURI = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), name);
      localizeResources.put(name, new LocalizeResource(actualURI, archive));
    } catch (URISyntaxException e) {
      // Most of the URI is constructed from the passed URI. So ideally, this should not happen.
      // If it does though, there is nothing that clients can do to recover, so not propagating a checked exception.
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return sparkRuntimeContext.getApplicationSpecification();
  }

  @Override
  public String getClusterName() {
    return sparkRuntimeContext.getClusterName();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return sparkRuntimeContext.getRuntimeArguments();
  }

  @Override
  public String getNamespace() {
    return sparkRuntimeContext.getNamespace();
  }

  @Override
  public RunId getRunId() {
    return sparkRuntimeContext.getRunId();
  }

  @Override
  public Admin getAdmin() {
    return sparkRuntimeContext.getAdmin();
  }

  @Override
  public DataTracer getDataTracer(String dataTracerName) {
    return sparkRuntimeContext.getDataTracer(dataTracerName);
  }

  @Nullable
  @Override
  public TriggeringScheduleInfo getTriggeringScheduleInfo() {
    return sparkRuntimeContext.getTriggeringScheduleInfo();
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(namespace, name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(name, arguments);
  }

  @Override
  public <T extends Dataset> T getDataset(String name, String namespace,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return sparkRuntimeContext.getDatasetCache().getDataset(namespace, name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    sparkRuntimeContext.getDatasetCache().releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    sparkRuntimeContext.getDatasetCache().discardDataset(dataset);
  }

  /**
   * Returns all the {@link LocalizeResource} that need to be localized for the Spark execution.
   *
   * @return a {@link Map} from resource name to {@link LocalizeResource}.
   */
  Map<String, LocalizeResource> getLocalizeResources() {
    return localizeResources;
  }

  /**
   * Returns the {@link Resources} requirement for the Spark driver process.
   */
  Resources getDriverResources() {
    return driverResources;
  }

  /**
   * Returns the {@link Resources} requirement for the Spark executor processes.
   */
  Resources getExecutorResources() {
    return executorResources;
  }

  /**
   * Returns the extra {@link SparkConf} to be passed to Spark submit.
   */
  @Nullable
  SparkConf getSparkConf() {
    return sparkConf;
  }

  /**
   * Extracts runtime arguments for the driver scope.
   */
  Map<String, String> getDriverRuntimeArguments() {
    return RuntimeArguments.extractScope("task", "driver", getRuntimeArguments());
  }

  /**
   * Extracts runtime arguments for the executor scope.
   */
  Map<String, String> getExecutorRuntimeArguments() {
    return RuntimeArguments.extractScope("task", "executor", getRuntimeArguments());
  }

  @Nullable
  @Override
  public URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
    return sparkRuntimeContext.getServiceURL(namespaceId, applicationId, serviceId);
  }

  @Nullable
  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return sparkRuntimeContext.getServiceURL(applicationId, serviceId);
  }

  @Nullable
  @Override
  public URL getServiceURL(String serviceId) {
    return sparkRuntimeContext.getServiceURL(serviceId);
  }

  @Nullable
  @Override
  public HttpURLConnection openConnection(String namespaceId, String applicationId,
                                          String serviceId, String methodPath) throws IOException {
    return sparkRuntimeContext.openConnection(namespaceId, applicationId, serviceId, methodPath);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return sparkRuntimeContext.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) {
    return sparkRuntimeContext.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return sparkRuntimeContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return sparkRuntimeContext.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InstantiationException {
    return sparkRuntimeContext.newPluginInstance(pluginId, evaluator);
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return sparkRuntimeContext.list(namespace);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return sparkRuntimeContext.get(namespace, name);
  }

  @Override
  public ProgramState getState() {
    return state;
  }

  void setState(ProgramState state) {
    this.state = state;
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    sparkRuntimeContext.execute(runnable);
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    sparkRuntimeContext.execute(timeoutInSeconds, runnable);
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    return sparkRuntimeContext.getMessagePublisher();
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    return sparkRuntimeContext.getDirectMessagePublisher();
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    return sparkRuntimeContext.getMessageFetcher();
  }

  @Override
  public void setPySparkScript(String script, URI... additionalPythonFiles) {
    setPySparkScript(script, Arrays.asList(additionalPythonFiles));
  }

  @Override
  public void setPySparkScript(String script, Iterable<URI> additionalPythonFiles) {
    pySparkScript = script;
    pySparkScriptLocation = null;
    additionalPythonLocations.clear();
    Iterables.addAll(additionalPythonLocations, additionalPythonFiles);
  }

  @Override
  public void setPySparkScript(URI scriptLocation, URI... additionalPythonFiles) {
    setPySparkScript(scriptLocation, Arrays.asList(additionalPythonFiles));
  }

  @Override
  public void setPySparkScript(URI scriptLocation, Iterable<URI> additionalPythonFiles) {
    pySparkScriptLocation = scriptLocation;
    pySparkScript = null;
    additionalPythonLocations.clear();
    Iterables.addAll(additionalPythonLocations, additionalPythonFiles);
  }

  /**
   * Gets the python script content.
   *
   * @return the python script or {@code null} if it was not set
   */
  @Nullable
  String getPySparkScript() {
    return pySparkScript;
  }

  /**
   * Gets the python script location.
   *
   * @return the python script location or {@code null} if it was not set
   */
  @Nullable
  URI getPySparkScriptLocation() {
    return pySparkScriptLocation;
  }

  /**
   * Gets the list of addition python files.
   */
  List<URI> getAdditionalPythonLocations() {
    return additionalPythonLocations;
  }

  boolean isPySpark() {
    return getPySparkScript() != null || getPySparkScriptLocation() != null;
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
    return sparkRuntimeContext.getMetadata(metadataEntity);
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
    return sparkRuntimeContext.getMetadata(scope, metadataEntity);
  }

  @Override
  public void record(Collection<? extends Operation> operations) {
    sparkRuntimeContext.record(operations);
  }

  @Override
  public void flushLineage() {
    sparkRuntimeContext.flushLineage();
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
    sparkRuntimeContext.addProperties(metadataEntity, properties);
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, String... tags) {
    sparkRuntimeContext.addTags(metadataEntity, tags);
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {
    sparkRuntimeContext.addTags(metadataEntity, tags);
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    sparkRuntimeContext.removeMetadata(metadataEntity);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {
    sparkRuntimeContext.removeProperties(metadataEntity);
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... keys) {
    sparkRuntimeContext.removeProperties(metadataEntity, keys);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    sparkRuntimeContext.removeTags(metadataEntity);
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... tags) {
    sparkRuntimeContext.removeTags(metadataEntity, tags);
  }
}
