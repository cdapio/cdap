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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.StageContext;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import co.cask.cdap.etl.spec.StageSpec;
import com.google.common.base.Throwables;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
  private Schema inputSchema;
  private Schema outputSchema;
  private final Map<String, Schema> outputPortSchemas;
  protected final BasicArguments arguments;

  protected AbstractStageContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec) {
    this.pipelineRuntime = pipelineRuntime;
    this.stageSpec = stageSpec;
    this.stageMetrics = new DefaultStageMetrics(pipelineRuntime.getMetrics(), stageSpec.getName());
    // all plugins except joiners have just a single input schema
    this.inputSchema = stageSpec.getInputSchemas().isEmpty() ?
      null : stageSpec.getInputSchemas().values().iterator().next();
    Map<String, Schema> portSchemas = new HashMap<>();
    for (StageSpec.Port outputPort : stageSpec.getOutputPorts().values()) {
      if (outputPort.getPort() != null) {
        portSchemas.put(outputPort.getPort(), outputPort.getSchema());
      }
    }
    this.outputPortSchemas = Collections.unmodifiableMap(portSchemas);
    this.arguments = pipelineRuntime.getArguments();
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
    return CALLER.callUnchecked(() -> pipelineRuntime.getPluginContext().getPluginProperties(scopePluginId(pluginId)));
  }

  @Override
  public final <T> T newPluginInstance(final String pluginId) throws InstantiationException {
    try {
      return CALLER.call(() -> pipelineRuntime.getPluginContext().newPluginInstance(scopePluginId(pluginId)));
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, InstantiationException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final <T> Class<T> loadPluginClass(final String pluginId) {
    return CALLER.callUnchecked(() -> pipelineRuntime.getPluginContext().loadPluginClass(scopePluginId(pluginId)));
  }

  @Override
  public final PluginProperties getPluginProperties() {
    return CALLER.callUnchecked(() -> pipelineRuntime.getPluginContext().getPluginProperties(stageSpec.getName()));
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
    return outputSchema == null ? stageSpec.getOutputSchema() : outputSchema;
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
  public URL getServiceURL(final String applicationId, final String serviceId) {
    return CALLER.callUnchecked(() -> pipelineRuntime.getServiceDiscoverer().getServiceURL(applicationId, serviceId));
  }

  @Nullable
  @Override
  public URL getServiceURL(final String serviceId) {
    return CALLER.callUnchecked(() -> pipelineRuntime.getServiceDiscoverer().getServiceURL(serviceId));
  }

  @Override
  public void setOutputSchema(Schema outputSchema) {
    this.outputSchema = outputSchema;
  }
}
