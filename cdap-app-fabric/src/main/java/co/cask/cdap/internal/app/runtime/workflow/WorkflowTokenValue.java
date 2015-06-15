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

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Class representing the value for the key in {@link WorkflowToken}.
 */
final class WorkflowTokenValue {

  private final Map<String, String> nodeTokenValue = Maps.newHashMap();

  private String lastSetterNode;

  void putValue(String nodeName, String value) {
    nodeTokenValue.put(nodeName, value);
    lastSetterNode = nodeName;
  }

  @Nullable
  String getLastSetter() {
    return lastSetterNode;
  }

  @Nullable
  String getValue() {
    return nodeTokenValue.get(lastSetterNode);
  }

  Map<String, String> getAllValues() {
    return ImmutableMap.copyOf(nodeTokenValue);
  }
}

