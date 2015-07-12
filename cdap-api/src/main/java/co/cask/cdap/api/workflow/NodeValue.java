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

package co.cask.cdap.api.workflow;

import java.io.Serializable;

/**
 * Multiple nodes in the Workflow can add the same key to the {@link WorkflowToken}.
 * This class provides a mapping from node name to the {@link Value} which was set for the
 * specific key.
 */
public final class NodeValue implements Serializable {

  private static final long serialVersionUID = 6157808964174399650L;

  private final String nodeName;
  private final Value value;

  public NodeValue(String nodeName, Value value) {
    this.nodeName = nodeName;
    this.value = value;
  }

  public String getNodeName() {
    return nodeName;
  }

  public Value getValue() {
    return value;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("NodeValue {");
    sb.append("nodeName=").append(nodeName);
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NodeValue that = (NodeValue) o;
    return nodeName.equals(that.nodeName) && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int result = nodeName.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
