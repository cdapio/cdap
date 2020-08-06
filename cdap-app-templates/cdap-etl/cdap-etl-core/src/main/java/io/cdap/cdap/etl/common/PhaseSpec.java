/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import java.io.Serializable;
import java.util.Map;

/**
 * Class that contain common pipeline phase information
 */
public class PhaseSpec implements Serializable {
  private final String name;
  private final PipelinePhase phase;
  private final Map<String, String> connectorDatasets;
  private final boolean isStageLoggingEnabled;
  private final boolean isProcessTimingEnabled;

  public PhaseSpec(String name, PipelinePhase phase, Map<String, String> connectorDatasets,
                   boolean isStageLoggingEnabled, boolean isProcessTimingEnabled) {
    this.name = name;
    this.phase = phase;
    this.connectorDatasets = connectorDatasets;
    this.isStageLoggingEnabled = isStageLoggingEnabled;
    this.isProcessTimingEnabled = isProcessTimingEnabled;
  }

  public PipelinePhase getPhase() {
    return phase;
  }

  public boolean isStageLoggingEnabled() {
    return isStageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return isProcessTimingEnabled;
  }

  public String getPhaseName() {
    return name;
  }

  public Map<String, String> getConnectorDatasets() {
    return connectorDatasets;
  }
}
