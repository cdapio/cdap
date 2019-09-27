/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.NoStageLoggingCaller;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import io.cdap.cdap.etl.validation.LoggingFailureCollector;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Base implementation of {@link StageContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractStageContext implements StageContext {

  private static final Caller CALLER = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  private final PipelineRuntime pipelineRuntime;
  private final StageSpec stageSpec;
  private final StageMetrics stageMetrics;
  private final Schema inputSchema;
  private final Map<String, Schema> outputPortSchemas;
  private final MacroEvaluator macroEvaluator;
  private final FailureCollector failureCollector;
  protected final BasicArguments arguments;

  protected AbstractStageContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    this.pipelineRuntime = pipelineRuntime;
    this.stageSpec = stageSpec;
    this.stageMetrics = new DefaultStageMetrics(pipelineRuntime.getMetrics(), stageSpec.getName());
    Map<String, Schema> inputSchemas = stageSpec.getInputSchemas();
    // all plugins except joiners have just a single input schema
    this.inputSchema = inputSchemas.isEmpty() ?
      null : stageSpec.getInputSchemas().values().iterator().next();
    Map<String, Schema> portSchemas = new HashMap<>();
    for (StageSpec.Port outputPort : stageSpec.getOutputPorts().values()) {
      if (outputPort.getPort() != null) {
        portSchemas.put(outputPort.getPort(), outputPort.getSchema());
      }
    }
    this.outputPortSchemas = Collections.unmodifiableMap(portSchemas);
    this.arguments = pipelineRuntime.getArguments();
    this.failureCollector = new LoggingFailureCollector(stageSpec.getName(), inputSchemas);
    this.macroEvaluator = new DefaultMacroEvaluator(arguments, pipelineRuntime.getLogicalStartTime(),
                                                    pipelineRuntime.getSecureStore(), pipelineRuntime.getNamespace());
  }

  @Override
  public String getNamespace() {
    return pipelineRuntime.getNamespace();
  }

  @Override
  public String getPipelineName() {
    return pipelineRuntime.getPipelineName();
  }

  @Override
  public long getLogicalStartTime() {
    return pipelineRuntime.getLogicalStartTime();
  }

  @Override
  public final PluginProperties getPluginProperties(final String pluginId) {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getPluginContext().getPluginProperties(scopePluginId(pluginId)));
  }

  @Override
  public final <T> T newPluginInstance(final String pluginId) throws InstantiationException {
    try {
      return CALLER.call(
        () -> pipelineRuntime.getPluginContext().newPluginInstance(scopePluginId(pluginId), macroEvaluator));
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, InstantiationException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final <T> Class<T> loadPluginClass(final String pluginId) {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getPluginContext().loadPluginClass(scopePluginId(pluginId)));
  }

  @Override
  public final PluginProperties getPluginProperties() {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getPluginContext().getPluginProperties(stageSpec.getName()));
  }

  @Override
  public final String getStageName() {
    return stageSpec.getName();
  }

  @Override
  public final StageMetrics getMetrics() {
    return stageMetrics;
  }

  @Nullable
  @Override
  public Schema getInputSchema() {
    return inputSchema;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return stageSpec.getInputSchemas();
  }

  @Nullable
  @Override
  public Schema getOutputSchema() {
    return stageSpec.getOutputSchema();
  }

  @Override
  public Map<String, Schema> getOutputPortSchemas() {
    return outputPortSchemas;
  }

  @Override
  public BasicArguments getArguments() {
    return arguments;
  }

  private String scopePluginId(String childPluginId) {
    return String.format("%s%s%s", stageSpec.getName(), Constants.ID_SEPARATOR, childPluginId);
  }

  @Nullable
  @Override
  public URL getServiceURL(final String namespaceId, final String applicationId, final String serviceId) {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getServiceDiscoverer().getServiceURL(namespaceId, applicationId, serviceId));
  }

  @Nullable
  @Override
  public URL getServiceURL(final String applicationId, final String serviceId) {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getServiceDiscoverer().getServiceURL(applicationId, serviceId));
  }

  @Nullable
  @Override
  public URL getServiceURL(final String serviceId) {
    return CALLER.callUnchecked(
      () -> pipelineRuntime.getServiceDiscoverer().getServiceURL(serviceId));
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
    return CALLER.callUnchecked(() -> getMetadataReader().getMetadata(metadataEntity));
  }

  @Override
  public Metadata getMetadata(MetadataScope scope, MetadataEntity metadataEntity) {
    return CALLER.callUnchecked(() -> getMetadataReader().getMetadata(scope, metadataEntity));
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().addProperties(metadataEntity, properties);
      return null;
    });
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, String... tags) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().addTags(metadataEntity, tags);
      return null;
    });
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Iterable<String> tags) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().addTags(metadataEntity, tags);
      return null;
    });
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().removeMetadata(metadataEntity);
      return null;
    });
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().removeProperties(metadataEntity);
      return null;
    });
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... keys) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().removeProperties(metadataEntity, keys);
      return null;
    });
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().removeTags(metadataEntity);
      return null;
    });
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... tags) {
    CALLER.callUnchecked((Callable<Void>) () -> {
      getMetadataWriter().removeTags(metadataEntity, tags);
      return null;
    });
  }

  @Override
  public FailureCollector getFailureCollector() {
    return failureCollector;
  }

  private MetadataReader getMetadataReader() {
    return pipelineRuntime.getMetadataReader().orElseThrow(this::createMetadataUnsupportedException);
  }

  private MetadataWriter getMetadataWriter() {
    return pipelineRuntime.getMetadataWriter().orElseThrow(this::createMetadataUnsupportedException);
  }

  private UnsupportedOperationException createMetadataUnsupportedException() {
    return new UnsupportedOperationException("Metadata operation is not supported.");
  }
}
