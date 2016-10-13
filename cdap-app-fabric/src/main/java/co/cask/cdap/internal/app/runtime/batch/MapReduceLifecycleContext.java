package co.cask.cdap.internal.app.runtime.batch;

/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.ProgramState;
import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.workflow.WorkflowInfo;
import co.cask.cdap.api.workflow.WorkflowToken;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
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

  @Nullable
  @Override
  public String getInputName() {
    return delegate.getInputName();
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
  public String getNamespace() {
    return delegate.getNamespace();
  }

  @Override
  public RunId getRunId() {
    return delegate.getRunId();
  }

  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return delegate.getServiceURL(applicationId, serviceId);
  }

  @Override
  public URL getServiceURL(String serviceId) {
    return delegate.getServiceURL(serviceId);
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
  public void setInput(StreamBatchReadable stream) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(String datasetName) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(String datasetName, List<Split> splits) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(String datasetName, Map<String, String> arguments, List<Split> splits) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(InputFormatProvider inputFormatProvider) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
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
  public void addOutput(String datasetName) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void addOutput(String datasetName, Map<String, String> arguments) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void addOutput(String alias, OutputFormatProvider outputFormatProvider) {
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
  public Map<String, String> listSecureData(String namespace) throws Exception {
    return delegate.listSecureData(namespace);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws Exception {
    return delegate.getSecureData(namespace, name);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    throw new TransactionFailureException("Attempt to start a transaction within active transaction " +
                                            delegate.getTransaction().getTransactionId());
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
    throw new TransactionFailureException("Attempt to start a transaction within active transaction " +
                                            delegate.getTransaction().getTransactionId());
  }

  @Override
  public String toString() {
    return "MapReduceLifecycleContext{" +
      "delegate=" + delegate +
      '}';
  }
}
