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
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken {
  private Map<String, Map<String, Long>> mapReduceCounters = Maps.newHashMap();

  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return mapReduceCounters;
  }

  public void setMapReduceCounters(Map<String, Map<String, Long>> mapReduceCounters) {
    this.mapReduceCounters = mapReduceCounters;
  }

  /**
   * Make a deep copy of the WorkflowToken. Currently only copies the MapReduce counters.
   * @param token WorkflowToken to be copy
   * @return copied WorkflowToken
   */
  public WorkflowToken deepCopy(WorkflowToken token) {
    BasicWorkflowToken copiedToken = new BasicWorkflowToken();
    Map<String, Map<String, Long>> copiedMap = Maps.newHashMap();
    for (Map.Entry<String, Map<String, Long>> entry : token.getMapReduceCounters().entrySet()) {
      copiedMap.put(entry.getKey(), new HashMap<String, Long>(entry.getValue()));
    }
    copiedToken.setMapReduceCounters(copiedMap);
    return copiedToken;
  }
}
