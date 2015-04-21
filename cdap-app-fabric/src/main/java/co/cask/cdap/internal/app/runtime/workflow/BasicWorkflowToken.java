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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken {
  private Map<String, Map<String, Long>> mapReduceCounters;

  @Nullable
  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return mapReduceCounters;
  }

  public void setMapReduceCounters(Map<String, Map<String, Long>> mapReduceCounters) {
    this.mapReduceCounters = copyHadoopCounters(mapReduceCounters);
  }

  /**
   * Make a deep copy of the WorkflowToken. Currently only copies the MapReduce counters.
   * @return copied WorkflowToken
   */
  public WorkflowToken deepCopy() {
    BasicWorkflowToken copiedToken = new BasicWorkflowToken();
    if (getMapReduceCounters() != null) {
      copiedToken.setMapReduceCounters(copyHadoopCounters(getMapReduceCounters()));
    }
    return copiedToken;
  }

  private Map<String, Map<String, Long>> copyHadoopCounters(Map<String, Map<String, Long>> input) {
    ImmutableMap.Builder<String, Map<String, Long>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Map<String, Long>> entry : input.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }
}
