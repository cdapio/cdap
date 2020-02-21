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

package io.cdap.cdap.etl.batch;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.etl.common.PhaseSpec;
import io.cdap.cdap.etl.common.PipelinePhase;

import java.util.Map;

/**
 * Information required by one phase of a batch pipeline.
 */
public class BatchPhaseSpec extends PhaseSpec {
  private final Resources resources;
  private final Resources driverResources;
  private final Resources clientResources;
  private final Map<String, String> pipelineProperties;
  private final String description;
  private final int numOfRecordsPreview;
  private final boolean isPipelineContainsCondition;

  public BatchPhaseSpec(String phaseName, PipelinePhase phase,
                        Resources resources, Resources driverResources, Resources clientResources,
                        boolean isStageLoggingEnabled, boolean isProcessTimingEnabled,
                        Map<String, String> connectorDatasets, int numOfRecordsPreview,
                        Map<String, String> pipelineProperties, boolean isPipelineContainsCondition) {
    super(phaseName, phase, connectorDatasets, isStageLoggingEnabled, isProcessTimingEnabled);
    this.resources = resources;
    this.driverResources = driverResources;
    this.clientResources = clientResources;
    this.description = createDescription();
    this.numOfRecordsPreview = numOfRecordsPreview;
    this.pipelineProperties = ImmutableMap.copyOf(pipelineProperties);
    this.isPipelineContainsCondition = isPipelineContainsCondition;
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
    Joiner.on("', '").appendTo(description, getPhase().getSources());
    description.append("' to sinks '");

    Joiner.on("', '").appendTo(description, getPhase().getSinks());
    description.append("'.");
    return description.toString();
  }
}
