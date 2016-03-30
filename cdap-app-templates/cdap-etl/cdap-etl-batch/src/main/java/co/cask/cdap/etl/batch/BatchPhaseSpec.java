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

import java.util.Map;

/**
 * Information required by one phase of a batch pipeline.
 */
public class BatchPhaseSpec {
  private final String phaseName;
  private final PipelinePhase phase;
  private final Resources resources;
  private final boolean isStageLoggingEnabled;
  private final Map<String, String> connectorDatasets;
  private final String description;

  public BatchPhaseSpec(String phaseName, PipelinePhase phase, Resources resources, boolean isStageLoggingEnabled,
                        Map<String, String> connectorDatasets) {
    this.phaseName = phaseName;
    this.phase = phase;
    this.resources = resources;
    this.isStageLoggingEnabled = isStageLoggingEnabled;
    this.connectorDatasets = connectorDatasets;
    this.description = createDescription();
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

  public boolean isStageLoggingEnabled() {
    return isStageLoggingEnabled;
  }

  public Map<String, String> getConnectorDatasets() {
    return connectorDatasets;
  }

  public String getDescription() {
    return description;
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
