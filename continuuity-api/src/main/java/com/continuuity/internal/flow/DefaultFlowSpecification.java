/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.flow;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 *
 */
public final class DefaultFlowSpecification implements FlowSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final Map<String, FlowletDefinition> flowlets;
  private final List<FlowletConnection> connections;

  public DefaultFlowSpecification(String name, String description,
                                  Map<String, FlowletDefinition> flowlets, List<FlowletConnection> connections) {
    this(null, name, description, flowlets, connections);
  }

  public DefaultFlowSpecification(String className, FlowSpecification other) {
    this(className, other.getName(), other.getDescription(), other.getFlowlets(), other.getConnections());
  }

  public DefaultFlowSpecification(String className, String name, String description,
                                  Map<String, FlowletDefinition> flowlets, List<FlowletConnection> connections) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.flowlets = ImmutableMap.copyOf(flowlets);
    this.connections = ImmutableList.copyOf(connections);
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, FlowletDefinition> getFlowlets() {
    return flowlets;
  }

  @Override
  public List<FlowletConnection> getConnections() {
    return connections;
  }
}
