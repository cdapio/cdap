/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to represent {@link WorkflowToken} for a give workflow run.
 */
public class WorkflowTokenDetail {

  private final Map<String, List<NodeValueDetail>> tokenData;

  public WorkflowTokenDetail(Map<String, List<NodeValueDetail>> tokenData) {
    this.tokenData = deepCopy(tokenData);
  }

  public Map<String, List<NodeValueDetail>> getTokenData() {
    return tokenData;
  }

  private static Map<String, List<NodeValueDetail>> deepCopy(Map<String, List<NodeValueDetail>> tokenData) {
    Map<String, List<NodeValueDetail>> tokenDataCopy = new LinkedHashMap<>();
    for (Map.Entry<String, List<NodeValueDetail>> entry : tokenData.entrySet()) {
      tokenDataCopy.put(entry.getKey(), Collections.unmodifiableList(new ArrayList<>(entry.getValue())));
    }
    return Collections.unmodifiableMap(tokenDataCopy);
  }

  /**
   * Constructs a {@link WorkflowTokenDetail} by flattening the {@link Value} from the output of
   * {@link WorkflowToken#getAll(WorkflowToken.Scope)}.
   *
   * @param data the workflow token data to flatten
   * @return {@link WorkflowTokenDetail} from the specified workflow token data
   */
  public static WorkflowTokenDetail of(Map<String, List<NodeValue>> data) {
    Map<String, List<NodeValueDetail>> converted = new HashMap<>();
    for (Map.Entry<String, List<NodeValue>> entry : data.entrySet()) {
      List<NodeValueDetail> convertedList = new ArrayList<>();
      for (NodeValue nodeValue : entry.getValue()) {
        convertedList.add(new NodeValueDetail(nodeValue.getNodeName(), nodeValue.getValue().toString()));
      }
      converted.put(entry.getKey(), convertedList);
    }
    return new WorkflowTokenDetail(converted);
  }

  /**
   * Class to represent node -> value mapping for a given key in a {@link WorkflowToken}.
   */
  public static class NodeValueDetail {
    private final String node;
    private final String value;

    public NodeValueDetail(String node, String value) {
      this.node = node;
      this.value = value;
    }

    public String getNode() {
      return node;
    }

    public String getValue() {
      return value;
    }
  }
}
