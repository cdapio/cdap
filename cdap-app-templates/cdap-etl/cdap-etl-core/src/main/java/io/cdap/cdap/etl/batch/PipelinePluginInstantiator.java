/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.AlertPublisher;
import io.cdap.cdap.etl.batch.connector.AlertPublisherSink;
import io.cdap.cdap.etl.batch.connector.ConnectorFactory;
import io.cdap.cdap.etl.batch.connector.ConnectorSink;
import io.cdap.cdap.etl.batch.connector.ConnectorSource;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.plugin.PipelinePluginContext;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Layer between the ETL programs and CDAP PluginContext to instantiate plugins for a stage in a pipeline.
 * This is required because {@link ConnectorSource} and {@link ConnectorSink} are not plugins because we want to
 * them to be internal only.
 */
public class PipelinePluginInstantiator implements PluginContext {
  private final PluginContext pluginContext;
  private final PhaseSpec phaseSpec;
  private final Set<String> connectorSources;
  private final Set<String> connectorSinks;
  private final ConnectorFactory connectorFactory;

  public PipelinePluginInstantiator(PluginContext pluginContext, Metrics metrics, PhaseSpec phaseSpec,
                                    ConnectorFactory connectorFactory) {
    this.pluginContext = new PipelinePluginContext(pluginContext, metrics,
                                                   phaseSpec.isStageLoggingEnabled(),
                                                   phaseSpec.isProcessTimingEnabled());
    this.phaseSpec = phaseSpec;
    this.connectorSources = new HashSet<>();
    this.connectorSinks = new HashSet<>();
    this.connectorFactory = connectorFactory;
    for (StageSpec connectorStage : phaseSpec.getPhase().getStagesOfType(Constants.Connector.PLUGIN_TYPE)) {
      String connectorName = connectorStage.getName();
      if (phaseSpec.getPhase().getSources().contains(connectorName)) {
        connectorSources.add(connectorName);
      }
      if (phaseSpec.getPhase().getSinks().contains(connectorName)) {
        connectorSinks.add(connectorName);
      }
    }
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return pluginContext.getPluginProperties(pluginId);
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId, MacroEvaluator evaluator) throws InvalidMacroException {
    return pluginContext.getPluginProperties(pluginId, evaluator);
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return pluginContext.loadPluginClass(pluginId);
  }

  @Override
  public <T> T newPluginInstance(String stageName) throws InstantiationException {
    T plugin = getBuiltIn(stageName);
    if (plugin != null) {
      return plugin;
    }

    return pluginContext.newPluginInstance(stageName);
  }

  @Override
  public <T> T newPluginInstance(String stageName, MacroEvaluator macroEvaluator) throws InstantiationException {
    T plugin = getBuiltIn(stageName);
    if (plugin != null) {
      return plugin;
    }

    return pluginContext.newPluginInstance(stageName, macroEvaluator);
  }

  @Nullable
  private <T> T getBuiltIn(String stageName) {
    if (connectorSources.contains(stageName)) {
      String datasetName = phaseSpec.getConnectorDatasets().get(stageName);
      return (T) connectorFactory.createSource(datasetName);
    } else if (connectorSinks.contains(stageName)) {
      String datasetName = phaseSpec.getConnectorDatasets().get(stageName);
      return (T) connectorFactory.createSink(datasetName, phaseSpec.getPhaseName());
    }
    StageSpec stageSpec = phaseSpec.getPhase().getStage(stageName);
    if (stageSpec != null && stageSpec.getPluginType().equals(AlertPublisher.PLUGIN_TYPE)) {
      String datasetName = phaseSpec.getConnectorDatasets().get(stageName);
      return (T) new AlertPublisherSink(datasetName, phaseSpec.getPhaseName());
    }
    return null;
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return pluginContext.isFeatureEnabled(name);
  }
}
