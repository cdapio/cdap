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
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Arguments;
import co.cask.cdap.etl.api.StageContext;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.ClassLoaderCaller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import co.cask.cdap.etl.planner.StageInfo;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Base implementation of {@link StageContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractStageContext implements StageContext {

  private final PluginContext pluginContext;
  private final String stageName;
  private final StageMetrics metrics;
  private final Map<String, Schema> inputSchemas;
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final Caller caller;
  protected final BasicArguments arguments;

  protected AbstractStageContext(PluginContext pluginContext, Metrics metrics,
                                 StageInfo stageInfo, BasicArguments arguments) {
    this.pluginContext = pluginContext;
    this.stageName = stageInfo.getName();
    this.metrics = new DefaultStageMetrics(metrics, stageName);
    this.outputSchema = stageInfo.getOutputSchema();
    this.inputSchemas = Collections.unmodifiableMap(stageInfo.getInputSchemas());
    // all plugins except joiners have just a single input schema
    this.inputSchema = inputSchemas.isEmpty() ? null : inputSchemas.values().iterator().next();
    this.arguments = arguments;
    this.caller = ClassLoaderCaller.wrap(NoStageLoggingCaller.wrap(Caller.DEFAULT), getClass().getClassLoader());
  }

  @Override
  public final PluginProperties getPluginProperties(final String pluginId) {
    return caller.callUnchecked(new Callable<PluginProperties>() {
      @Override
      public PluginProperties call() throws Exception {
        return pluginContext.getPluginProperties(scopePluginId(pluginId));
      }
    });
  }

  @Override
  public final <T> T newPluginInstance(final String pluginId) throws InstantiationException {
    try {
      return caller.call(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return pluginContext.newPluginInstance(scopePluginId(pluginId));
        }
      });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, InstantiationException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public final <T> Class<T> loadPluginClass(final String pluginId) {
    return caller.callUnchecked(new Callable<Class<T>>() {
      @Override
      public Class<T> call() throws Exception {
        return pluginContext.loadPluginClass(scopePluginId(pluginId));
      }
    });
  }

  @Override
  public final PluginProperties getPluginProperties() {
    return caller.callUnchecked(new Callable<PluginProperties>() {
      @Override
      public PluginProperties call() throws Exception {
        return pluginContext.getPluginProperties(stageName);
      }
    });
  }

  @Override
  public final String getStageName() {
    return stageName;
  }

  @Override
  public final StageMetrics getMetrics() {
    return metrics;
  }

  @Nullable
  @Override
  public Schema getInputSchema() {
    return inputSchema;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return inputSchemas;
  }

  @Nullable
  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public Arguments getArguments() {
    return arguments;
  }

  private String scopePluginId(String childPluginId) {
    return String.format("%s%s%s", stageName, Constants.ID_SEPARATOR, childPluginId);
  }

}
