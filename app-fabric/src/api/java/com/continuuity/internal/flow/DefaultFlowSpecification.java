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
