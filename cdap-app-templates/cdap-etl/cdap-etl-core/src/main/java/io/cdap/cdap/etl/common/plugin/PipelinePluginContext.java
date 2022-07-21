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

package io.cdap.cdap.etl.common.plugin;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.ErrorTransform;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchJoiner;
import io.cdap.cdap.etl.api.batch.BatchReducibleAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.cdap.etl.common.DefaultStageMetrics;

/**
 * Creates pipeline plugins. Any call made on the plugins will be wrapped so that the context classloader is set
 * to the plugin's classloader, the stage name will be injected into log messages, and metrics on time spent will
 * be emitted.
 */
@SuppressWarnings("unchecked")
public class PipelinePluginContext implements PluginContext {
  private final PluginContext delegate;
  private final Metrics metrics;
  private final boolean stageLoggingEnabled;
  private final boolean processTimingEnabled;

  public PipelinePluginContext(PluginContext delegate, Metrics metrics,
                               boolean stageLoggingEnabled, boolean processTimingEnabled) {
    this.delegate = delegate;
    this.metrics = metrics;
    this.stageLoggingEnabled = stageLoggingEnabled;
    this.processTimingEnabled = processTimingEnabled;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return delegate.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return delegate.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return delegate.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return (T) wrapPlugin(pluginId, delegate.newPluginInstance(pluginId));
  }

  @Override
  public <T> T newPluginInstance(String pluginId,
                                 MacroEvaluator evaluator) throws InstantiationException, InvalidMacroException {
    return (T) wrapPlugin(pluginId, delegate.newPluginInstance(pluginId, evaluator));
  }

  private Object wrapPlugin(String pluginId, Object plugin) {
    Caller caller = getCaller(pluginId);
    StageMetrics stageMetrics = new DefaultStageMetrics(metrics, pluginId);
    OperationTimer operationTimer =
      processTimingEnabled ? new MetricsOperationTimer(stageMetrics) : NoOpOperationTimer.INSTANCE;
    if (plugin instanceof Action) {
      return new WrappedAction((Action) plugin, caller);
    } else if (plugin instanceof BatchSource) {
      return new WrappedBatchSource<>((BatchSource) plugin, caller, operationTimer);
    } else if (plugin instanceof BatchSink) {
      return new WrappedBatchSink<>((BatchSink) plugin, caller, operationTimer);
    } else if (plugin instanceof ErrorTransform) {
      return new WrappedErrorTransform<>((ErrorTransform) plugin, caller, operationTimer);
    } else if (plugin instanceof Transform) {
      return new WrappedTransform<>((Transform) plugin, caller, operationTimer);
    } else if (plugin instanceof BatchReducibleAggregator) {
      return new WrappedReduceAggregator<>((BatchReducibleAggregator) plugin, caller, operationTimer);
    } else if (plugin instanceof BatchAggregator) {
      return new WrappedBatchAggregator<>((BatchAggregator) plugin, caller, operationTimer);
    } else if (plugin instanceof BatchJoiner) {
      return new WrappedBatchJoiner<>((BatchJoiner) plugin, caller, operationTimer);
    } else if (plugin instanceof PostAction) {
      return new WrappedPostAction((PostAction) plugin, caller);
    } else if (plugin instanceof SplitterTransform) {
      return new WrappedSplitterTransform<>((SplitterTransform) plugin, caller, operationTimer);
    }

    return wrapUnknownPlugin(pluginId, plugin, caller);
  }

  public Caller getCaller(String pluginId) {
    Caller caller = Caller.DEFAULT;
    if (stageLoggingEnabled) {
      caller = StageLoggingCaller.wrap(caller, pluginId);
    }
    return caller;
  }

  protected Object wrapUnknownPlugin(String pluginId, Object plugin, Caller caller) {
    return plugin;
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return delegate.isFeatureEnabled(name);
  }
}
