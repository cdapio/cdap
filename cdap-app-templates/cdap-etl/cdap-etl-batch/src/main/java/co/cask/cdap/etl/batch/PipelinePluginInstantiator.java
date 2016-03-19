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

import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.etl.batch.connector.ConnectorSink;
import co.cask.cdap.etl.batch.connector.ConnectorSource;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.HashSet;
import java.util.Set;

/**
 * Layer between the ETL programs and CDAP PluginContext to instantiate plugins for a stage in a pipeline.
 * This is required because {@link ConnectorSource} and {@link ConnectorSink} are not plugins because we want to
 * them to be internal only.
 */
public class PipelinePluginInstantiator {
  private final PluginContext pluginContext;
  private final BatchPhaseSpec phaseSpec;
  private final Set<String> connectorSources;
  private final Set<String> connectorSinks;

  public PipelinePluginInstantiator(PluginContext pluginContext, BatchPhaseSpec phaseSpec) {
    this.pluginContext = pluginContext;
    this.phaseSpec = phaseSpec;
    this.connectorSources = new HashSet<>();
    this.connectorSinks = new HashSet<>();
    for (StageInfo connectorStage : phaseSpec.getPhase().getStagesOfType(Constants.CONNECTOR_TYPE)) {
      String connectorName = connectorStage.getName();
      if (phaseSpec.getPhase().getSources().contains(connectorName)) {
        connectorSources.add(connectorName);
      }
      if (phaseSpec.getPhase().getSinks().contains(connectorName)) {
        connectorSinks.add(connectorName);
      }
    }
  }

  public <T> T newPluginInstance(String stageName) throws InstantiationException {
    if (connectorSources.contains(stageName)) {
      String datasetName = phaseSpec.getConnectorDatasets().get(stageName);
      return (T) new ConnectorSource(datasetName, null);
    } else if (connectorSinks.contains(stageName)) {
      String datasetName = phaseSpec.getConnectorDatasets().get(stageName);
      return (T) new ConnectorSink(datasetName, phaseSpec.getPhaseName(), true);
    }

    return pluginContext.newPluginInstance(stageName);
  }
}
