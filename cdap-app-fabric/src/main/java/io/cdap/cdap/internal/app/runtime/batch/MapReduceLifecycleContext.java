/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputContext;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.mapreduce.MapReduceContext;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.api.messaging.MessageFetcher;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataException;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implements both MapReduceContext and MapReduceTaskContext to support backwards compatibility of Mapper/Reducer tasks
 * that implemented ProgramLifeCycle<MapReduceContext>.
 *
 * @param <KEY>   Type of output key.
 * @param <VALUE> Type of output value.
 */
public class MapReduceLifecycleContext<KEY, VALUE> implements MapReduceTaskContext<KEY, VALUE>, MapReduceContext {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceLifecycleContext.class);
  private final BasicMapReduceTaskContext<KEY, VALUE> delegate;

  public MapReduceLifecycleContext(BasicMapReduceTaskContext<KEY, VALUE> delegate) {
    this.delegate = delegate;
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    delegate.write(namedOutput, key, value);
  }

  @Override
  public void write(KEY key, VALUE value) throws IOException, InterruptedException {
    delegate.write(key, value);
  }

  @Override
  public MapReduceSpecification getSpecification() {
    return delegate.getSpecification();
  }

  @Override
  public long getLogicalStartTime() {
    return delegate.getLogicalStartTime();
  }

  @Override
  public <T> T getHadoopContext() {
    return delegate.getHadoopContext();
  }

  @Nullable
  @Override
  public WorkflowToken getWorkflowToken() {
    return delegate.getWorkflowToken();
  }

  @Nullable
  @Override
  public WorkflowInfo getWorkflowInfo() {
    return delegate.getWorkflowInfo();
  }

  @Override
  public InputContext getInputContext() {
    return delegate.getInputContext();
  }

  /**
   *
   * @return a map of feature flag names and if they are enabled.
   */
  @Override
  public boolean isFeatureEnabled(String name) {
    return delegate.isFeatureEnabled(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return delegate.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
    return delegate.getDataset(namespace, name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return delegate.getDataset(name, arguments);
  }

  @Override
  public <T extends Dataset> T getDataset(String namespace, String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return delegate.getDataset(namespace, name, arguments);
  }

  @Override
  public void releaseDataset(Dataset dataset) {
    delegate.releaseDataset(dataset);
  }

  @Override
  public void discardDataset(Dataset dataset) {
    delegate.discardDataset(dataset);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return delegate.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) {
    return delegate.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return delegate.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return delegate.newPluginInstance(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) throws InstantiationException {
    return delegate.newPluginInstance(pluginId, evaluator);
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return delegate.getApplicationSpecification();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return delegate.getRuntimeArguments();
  }

  @Override
  public String getClusterName() {
    return delegate.getClusterName();
  }

  @Override
  public String getNamespace() {
    return delegate.getNamespace();
  }

  @Override
  public RunId getRunId() {
    return delegate.getRunId();
  }

  @Override
  public URL getServiceURL(String namespaceId, String applicationId, String serviceId) {
    return delegate.getServiceURL(namespaceId, applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return delegate.getServiceURL(applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return delegate.getServiceURL(serviceId);
  }

  @Nullable
  @Override
  public HttpURLConnection openConnection(String namespaceId, String applicationId,
                                          String serviceId, String methodPath) throws IOException {
    return delegate.openConnection(namespaceId, applicationId, serviceId, methodPath);
  }

  // TODO: Document usage of ProgramLifeCycle<MapReduceTaskContext> instead of ProgramLifeCycle<MapReduceContext>
  // Until then, we can not remove the following methods.
  private static final String UNSUPPORTED_OPERATION_MESSAGE =
    "Operation not supported from MapReduce task-level context.";

  @Override
  public <T> T getHadoopJob() {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
    return null;
  }

  @Override
  public void addInput(Input input) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void addInput(Input input, Class<?> mapperCls) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void addOutput(Output output) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setMapperResources(Resources resources) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setReducerResources(Resources resources) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public ProgramState getState() {
    // In MapperWrapper and ReducerWrapper the status would be RUNNING
    return new ProgramState(ProgramStatus.RUNNING, null);
  }

  @Override
  public void localize(String name, URI uri) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void localize(String name, URI uri, boolean archive) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public File getLocalFile(String name) throws FileNotFoundException {
    return delegate.getLocalFile(name);
  }

  @Override
  public Map<String, File> getAllLocalFiles() {
    return delegate.getAllLocalFiles();
  }

  @Override
  public Admin getAdmin() {
    return delegate.getAdmin();
  }

  @Override
  public DataTracer getDataTracer(String dataTracerName) {
    return delegate.getDataTracer(dataTracerName);
  }

  @Nullable
  @Override
  public TriggeringScheduleInfo getTriggeringScheduleInfo() {
    return delegate.getTriggeringScheduleInfo();
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return delegate.list(namespace);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return delegate.get(namespace, name);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    throw new TransactionFailureException("Attempted to start a transaction within a MapReduce transaction");
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    throw new TransactionFailureException("Attempted to start a transaction within a MapReduce transaction");
  }

  @Override
  public String toString() {
    return "MapReduceLifecycleContext{" +
      "delegate=" + delegate +
      '}';
  }

  @Override
  public MessagePublisher getMessagePublisher() {
    // TODO: CDAP-7807
    throw new UnsupportedOperationException("Messaging is not supported in MapReduce task-level context");
  }

  @Override
  public MessagePublisher getDirectMessagePublisher() {
    // TODO: CDAP-7807
    throw new UnsupportedOperationException("Messaging is not supported in MapReduce task-level context");
  }

  @Override
  public MessageFetcher getMessageFetcher() {
    // TODO: CDAP-7807
    throw new UnsupportedOperationException("Messaging is not supported in MapReduce task-level context");
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) throws MetadataException {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) throws MetadataException {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void record(Collection<? extends Operation> operations) {
    throw new UnsupportedOperationException("Recording field lineage operations is not supported in " +
                                              "MapReduce task-level context");
  }

  @Override
  public void flushLineage() {
    throw new UnsupportedOperationException("Recording field lineage operations is not supported in " +
                                              "MapReduce task-level context");
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, String... tags) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... keys) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... tags) {
    throw new UnsupportedOperationException("Metadata operations are not supported from tasks.");
  }
}
