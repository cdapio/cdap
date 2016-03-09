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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.log.LogContext;
import com.google.common.base.Throwables;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Base implementation of {@link TransformContext} for common functionality.
 * This context scopes plugin ids by the id of the stage. This allows multiple transforms to use plugins with
 * the same id without clobbering each other.
 */
public abstract class AbstractTransformContext implements TransformContext {

  private final PluginContext pluginContext;
  private final String stageName;
  private final StageMetrics metrics;
  private final LookupProvider lookup;

  public AbstractTransformContext(PluginContext pluginContext,
                                  Metrics metrics, LookupProvider lookup, String stageName) {
    this.pluginContext = pluginContext;
    this.stageName = stageName;
    this.lookup = lookup;
    this.metrics = new DefaultStageMetrics(metrics, stageName);
  }

  @Override
  public final PluginProperties getPluginProperties(final String pluginId) {
    return LogContext.runWithoutLoggingUnchecked(new Callable<PluginProperties>() {
      @Override
      public PluginProperties call() throws Exception {
        return pluginContext.getPluginProperties(scopePluginId(pluginId));
      }
    });
  }

  @Override
  public final <T> T newPluginInstance(final String pluginId) throws InstantiationException {
    try {
      return LogContext.runWithoutLogging(new Callable<T>() {
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
    return LogContext.runWithoutLoggingUnchecked(new Callable<Class<T>>() {
      @Override
      public Class<T> call() throws Exception {
        return pluginContext.loadPluginClass(scopePluginId(pluginId));
      }
    });
  }

  @Override
  public final PluginProperties getPluginProperties() {
    return LogContext.runWithoutLoggingUnchecked(new Callable<PluginProperties>() {
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

  private String scopePluginId(String childPluginId) {
    return String.format("%s%s%s", stageName, Constants.ID_SEPARATOR, childPluginId);
  }

  @Override
  public <T> Lookup<T> provide(String table, Map<String, String> arguments) {
    return lookup.provide(table, arguments);
  }
}
