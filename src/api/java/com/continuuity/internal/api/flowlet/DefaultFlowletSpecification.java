package com.continuuity.internal.api.flowlet;

import com.continuuity.api.flow.flowlet.FailurePolicy;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

/**
 *
 */
public final class DefaultFlowletSpecification implements FlowletSpecification {

  private final String className;
  private final String name;
  private final String description;
  private final FailurePolicy failurePolicy;

  public DefaultFlowletSpecification(String name, String description, FailurePolicy failurePolicy) {
    this(null, name, description, failurePolicy);
  }

  public DefaultFlowletSpecification(String className, FlowletSpecification other) {
    this(className, other.getName(), other.getDescription(), other.getFailurePolicy());
  }

  public DefaultFlowletSpecification(String className, String name, String description, FailurePolicy failurePolicy) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.failurePolicy = failurePolicy;
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
}
