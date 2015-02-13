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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public final class WorkflowForkBranch {
  private final String name;
  private final List<WorkflowNode> nodes;

  public WorkflowForkBranch(String name, List<WorkflowNode> nodes) {
    this.name = name;
    this.nodes = Collections.unmodifiableList(new ArrayList<WorkflowNode>(nodes));
  }

  public String getName() {
    return name;
  }
  public List<WorkflowNode> getNodes() {
    return nodes;
  }
}
