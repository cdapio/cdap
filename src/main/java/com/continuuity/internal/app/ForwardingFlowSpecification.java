package com.continuuity.internal.app;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class ForwardingFlowSpecification implements FlowSpecification {

  private final FlowSpecification delegate;

  protected ForwardingFlowSpecification(FlowSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getClassName() {
    return delegate.getClassName();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public Map<String, FlowletDefinition> getFlowlets() {
    return delegate.getFlowlets();
  }

  @Override
  public List<FlowletConnection> getConnections() {
    return delegate.getConnections();
  }
}
