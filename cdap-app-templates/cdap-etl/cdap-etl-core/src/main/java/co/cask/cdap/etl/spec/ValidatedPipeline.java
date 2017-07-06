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

package co.cask.cdap.etl.spec;

import co.cask.cdap.etl.proto.Connection;
import co.cask.cdap.etl.proto.v2.ETLConfig;
import co.cask.cdap.etl.proto.v2.ETLStage;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to hold a stage with its input and outputs.
 */
public class ValidatedPipeline {
  // list of stages in the order they can be configured
  private final List<ETLStage> traversalOrder;
  // stage name -> output stage -> port
  private final Map<String, Map<String, String>> connectionTable;
  private final boolean stageLoggingEnabled;
  private final boolean processTimingEnabled;

  public ValidatedPipeline(List<ETLStage> traversalOrder, ETLConfig config) {
    this.traversalOrder = ImmutableList.copyOf(traversalOrder);
    this.connectionTable = new HashMap<>();
    for (Connection connection : config.getConnections()) {
      if (!connectionTable.containsKey(connection.getFrom())) {
        connectionTable.put(connection.getFrom(), new HashMap<String, String>());
      }
      Map<String, String> outputPorts = connectionTable.get(connection.getFrom());
      outputPorts.put(connection.getTo(), connection.getPort());
    }
    this.stageLoggingEnabled = config.isStageLoggingEnabled();
    this.processTimingEnabled = config.isProcessTimingEnabled();
  }

  public List<ETLStage> getTraversalOrder() {
    return traversalOrder;
  }

  public Set<String> getOutputs(String stageName) {
    return connectionTable.containsKey(stageName) ?
      connectionTable.get(stageName).keySet() : Collections.<String>emptySet();
  }

  public Map<String, String> getOutputPorts(String stageName) {
    return connectionTable.containsKey(stageName) ?
      connectionTable.get(stageName) : Collections.<String, String>emptyMap();
  }

  public boolean isStageLoggingEnabled() {
    return stageLoggingEnabled;
  }

  public boolean isProcessTimingEnabled() {
    return processTimingEnabled;
  }
}
