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
package co.cask.cdap.proto;

import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to represent {@link WorkflowToken} for a given workflow run at a given node.
 */
public class WorkflowTokenNodeDetail {

  private final Map<String, String> tokenDataAtNode;

  public WorkflowTokenNodeDetail(Map<String, String> tokenDataAtNode) {
    this.tokenDataAtNode = ImmutableMap.copyOf(tokenDataAtNode);
  }

  public Map<String, String> getTokenDataAtNode() {
    return tokenDataAtNode;
  }

  /**
   * Constructs a {@link WorkflowTokenNodeDetail} by flattening the {@link Value} from the output of
   * {@link WorkflowToken#getAllFromNode(String, WorkflowToken.Scope)}.
   *
   * @param data the workflow token data at a given node to flatten
   * @return {@link WorkflowTokenNodeDetail} from the given workflow token data at a node
   */
  public static WorkflowTokenNodeDetail of(Map<String, Value> data) {
    Map<String, String> flattened = new HashMap<>();
    for (Map.Entry<String, Value> entry : data.entrySet()) {
      flattened.put(entry.getKey(), entry.getValue().toString());
    }
    return new WorkflowTokenNodeDetail(flattened);
  }
}
