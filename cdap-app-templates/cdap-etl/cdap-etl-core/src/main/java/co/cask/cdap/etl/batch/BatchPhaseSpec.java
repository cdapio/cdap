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

import co.cask.cdap.api.Resources;
import co.cask.cdap.etl.common.PipelinePhase;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Information required by one phase of a batch pipeline.
 */
public class BatchPhaseSpec {
  private final String phaseName;
  private final PipelinePhase phase;
  private final Resources resources;
  private final Resources driverResources;
  private final Resources clientResources;
  private final boolean isStageLoggingEnabled;
  private final boolean isProcessTimingEnabled;
  private final Map<String, String> connectorDatasets;
  private final Map<String, String> pipelineProperties;
  private final String description;
  private final int numOfRecordsPreview;
  private final boolean isPipelineContainsCondition;

  public BatchPhaseSpec(String phaseName, PipelinePhase phase,
                        Resources resources, Resources driverResources, Resources clientResources,
                        boolean isStageLoggingEnabled, boolean isProcessTimingEnabled,
                        Map<String, String> connectorDatasets, int numOfRecordsPreview,
                        Map<String, String> pipelineProperties, boolean isPipelineContainsCondition) {
    this.phaseName = phaseName;
    this.phase = phase;
    this.resources = resources;
    this.driverResources = driverResources;
    this.clientResources = clientResources;
    this.isStageLoggingEnabled = isStageLoggingEnabled;
    this.isProcessTimingEnabled = isProcessTimingEnabled;
    this.connectorDatasets = connectorDatasets;
    this.description = createDescription();
    this.numOfRecordsPreview = numOfRecordsPreview;
    this.pipelineProperties = ImmutableMap.copyOf(pipelineProperties);
    this.isPipelineContainsCondition = isPipelineContainsCondition;
  }

  public String getPhaseName() {
    return phaseName;
  }

  public PipelinePhase getPhase() {
    return phase;
  }

  public Resources getResources() {
    return resources;
  }

  public Resources getDriverResources() {
    return driverResources;
  }

  public Resources getClientResources() {
    return clientResources;
  }

  public boolean isStageLoggingEnabled() {
    return isStageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return isProcessTimingEnabled;
  }

  public Map<String, String> getConnectorDatasets() {
    return connectorDatasets;
  }

  public String getDescription() {
    return description;
  }

  public int getNumOfRecordsPreview() {
    return numOfRecordsPreview;
  }

  public Map<String, String> getPipelineProperties() {
    return pipelineProperties;
  }

  public boolean pipelineContainsCondition() {
    return isPipelineContainsCondition;
  }

  private String createDescription() {
    StringBuilder description = new StringBuilder("Sources '");
    Joiner.on("', '").appendTo(description, phase.getSources());
    description.append("' to sinks '");

    Joiner.on("', '").appendTo(description, phase.getSinks());
    description.append("'.");
    return description.toString();
  }
}
