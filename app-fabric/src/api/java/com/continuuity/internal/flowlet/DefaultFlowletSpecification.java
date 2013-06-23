package com.continuuity.internal.flowlet;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DefaultFlowletSpecification implements FlowletSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final FailurePolicy failurePolicy;
  private final Set<String> dataSets;
  private final Map<String, String> arguments;

  public DefaultFlowletSpecification(String name, String description,
                                     FailurePolicy failurePolicy, Set<String> dataSets,
                                     Map<String, String> arguments) {
    this(null, name, description, failurePolicy, dataSets, arguments);
  }

  public DefaultFlowletSpecification(String className, FlowletSpecification other) {
    this(className, other.getName(), other.getDescription(),
         other.getFailurePolicy(), other.getDataSets(), other.getArguments());
  }

  public DefaultFlowletSpecification(String className, String name,
                                     String description, FailurePolicy failurePolicy,
                                     Set<String> dataSets, Map<String, String> arguments) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.failurePolicy = failurePolicy;
    this.dataSets = ImmutableSet.copyOf(dataSets);
    this.arguments = arguments == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(arguments);
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
  public FailurePolicy getFailurePolicy() {
    return failurePolicy;
  }

  @Override
  public Set<String> getDataSets() {
    return dataSets;
  }

  @Override
  public Map<String, String> getArguments() {
    return arguments;
  }
}
