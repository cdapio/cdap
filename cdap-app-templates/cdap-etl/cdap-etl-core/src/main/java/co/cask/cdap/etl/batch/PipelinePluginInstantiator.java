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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.batch.connector.AlertPublisherSink;
import co.cask.cdap.etl.batch.connector.ConnectorFactory;
import co.cask.cdap.etl.batch.connector.ConnectorSink;
import co.cask.cdap.etl.batch.connector.ConnectorSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.plugin.PipelinePluginContext;
import co.cask.cdap.etl.spec.StageSpec;

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
  private final BatchPhaseSpec phaseSpec;
  private final Set<String> connectorSources;
  private final Set<String> connectorSinks;
  private final ConnectorFactory connectorFactory;

  public PipelinePluginInstantiator(PluginContext pluginContext, Metrics metrics, BatchPhaseSpec phaseSpec,
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
    if (stageSpec.getPluginType().equals(AlertPublisher.PLUGIN_TYPE)) {
      return (T) new AlertPublisherSink(stageName, phaseSpec.getPhaseName());
    }
    return null;
  }
}
