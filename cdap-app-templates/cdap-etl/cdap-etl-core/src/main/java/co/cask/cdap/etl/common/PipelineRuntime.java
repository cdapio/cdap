/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.customaction.CustomActionContext;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.api.metadata.MetadataReader;
import co.cask.cdap.api.metadata.MetadataWriter;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.etl.api.StageContext;

import java.util.Optional;

/**
 * Holds common information, services, and contexts required at runtime by plugins. This exists mainly so that we
 * don't have modify several layers of constructors whenever anything is added to {@link StageContext}. Instead, it
 * can just be added here.
 */
public class PipelineRuntime {
  private final String namespace;
  private final String pipelineName;
  private final long logicalStartTime;
  private final BasicArguments arguments;
  private final Metrics metrics;
  private final PluginContext pluginContext;
  private final ServiceDiscoverer serviceDiscoverer;
  private final MetadataReader metadataReader;
  private final MetadataWriter metadataWriter;
  private final SecureStore secureStore;

  public PipelineRuntime(SparkClientContext context) {
    this(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
         new BasicArguments(context), context.getMetrics(), context, context, context, context, context);
  }

  public PipelineRuntime(CustomActionContext context, Metrics metrics) {
    this(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
         new BasicArguments(context), metrics, context, context, context, context, context);
  }

  public PipelineRuntime(MapReduceTaskContext context, Metrics metrics, BasicArguments arguments) {
    this(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
         arguments, metrics, context, context, context);
  }

  public PipelineRuntime(MapReduceContext context, Metrics metrics) {
    this(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
         new BasicArguments(context), metrics, context, context, context, context, context);
  }

  public PipelineRuntime(WorkflowContext context, Metrics metrics) {
    this(context.getNamespace(), context.getApplicationSpecification().getName(), context.getLogicalStartTime(),
         new BasicArguments(context.getToken(), context.getRuntimeArguments()), metrics, context, context, context,
         context, context);
  }

  public PipelineRuntime(String namespace, String pipelineName, long logicalStartTime, BasicArguments arguments,
                         Metrics metrics, PluginContext pluginContext, ServiceDiscoverer serviceDiscoverer,
                         SecureStore secureStore, MetadataReader metadataReader, MetadataWriter metadataWriter) {
    this.namespace = namespace;
    this.pipelineName = pipelineName;
    this.logicalStartTime = logicalStartTime;
    this.arguments = arguments;
    this.metrics = metrics;
    this.pluginContext = pluginContext;
    this.serviceDiscoverer = serviceDiscoverer;
    this.metadataReader = metadataReader;
    this.metadataWriter = metadataWriter;
    this.secureStore = secureStore;
  }

  public PipelineRuntime(String namespace, String pipelineName, long logicalStartTime, BasicArguments arguments,
                         Metrics metrics, PluginContext pluginContext, ServiceDiscoverer serviceDiscoverer,
                         SecureStore secureStore) {
    this(namespace, pipelineName, logicalStartTime, arguments, metrics, pluginContext, serviceDiscoverer,
         secureStore, null, null);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  public BasicArguments getArguments() {
    return arguments;
  }

  public Metrics getMetrics() {
    return metrics;
  }

  public PluginContext getPluginContext() {
    return pluginContext;
  }

  public ServiceDiscoverer getServiceDiscoverer() {
    return serviceDiscoverer;
  }

  public SecureStore getSecureStore() {
    return secureStore;
  }

  /**
   * @return an {@link Optional} of {@link MetadataReader} which is present is metadataReader is not null
   */
  public Optional<MetadataReader> getMetadataReader() {
    if (metadataReader != null) {
      return Optional.of(metadataReader);
    }
    return Optional.empty();
  }

  /**
   * @return an {@link Optional} of {@link MetadataWriter} which is present is metadataWriter is not null
   */
  public Optional<MetadataWriter> getMetadataWriter() {
    if (metadataWriter != null) {
      return Optional.of(metadataWriter);
    }
    return Optional.empty();
  }
}
