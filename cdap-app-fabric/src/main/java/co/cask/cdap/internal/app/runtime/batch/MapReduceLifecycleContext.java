package co.cask.cdap.internal.app.runtime.batch;

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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.templates.AdapterSpecification;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.workflow.WorkflowToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implements both MapReduceContext and MapReduceTaskContext to support backwards compatability of Mapper/Reducer tasks
 * that implemented ProgramLifeCycle<MapReduceContext>.
 *
 * @param <KEYOUT>   Type of output key.
 * @param <VALUEOUT> Type of output value.
 */
public class MapReduceLifecycleContext<KEYOUT, VALUEOUT> implements MapReduceTaskContext<KEYOUT, VALUEOUT>,
  MapReduceContext, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceLifecycleContext.class);
  private final BasicMapReduceTaskContext<KEYOUT, VALUEOUT> delegate;

  public MapReduceLifecycleContext(BasicMapReduceTaskContext<KEYOUT, VALUEOUT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    delegate.write(namedOutput, key, value);
  }

  @Override
  public void write(KEYOUT key, VALUEOUT value) throws IOException, InterruptedException {
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
  public AdapterSpecification getAdapterSpecification() {
    return delegate.getAdapterSpecification();
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
  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return delegate.getDataset(name);
  }

  @Override
  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    return delegate.getDataset(name, arguments);
  }

  @Override
  public PluginProperties getPluginProps(String pluginId) {
    return delegate.getPluginProps(pluginId);
  }

  @Override
  public <T> Class<T> loadClass(String pluginId) {
    return delegate.loadClass(pluginId);
  }

  @Override
  public <T> T newInstance(String pluginId) throws InstantiationException {
    return delegate.newInstance(pluginId);
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
  public void setInput(String datasetName, List<Split> splits) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setInput(String datasetName, Dataset dataset) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setOutput(String datasetName) {
    LOG.warn(UNSUPPORTED_OPERATION_MESSAGE);
  }

  @Override
  public void setOutput(String datasetName, Dataset dataset) {
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
  public void addOutput(String outputName, OutputFormatProvider outputFormatProvider) {
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
}
