package com.continuuity.internal.app;

import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Forwarding TwillSpecification implementation.
 */
public abstract class ForwardingTwillSpecification implements TwillSpecification {

  private final TwillSpecification delegate;

  protected ForwardingTwillSpecification(TwillSpecification specification) {
    this.delegate = specification;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return delegate.getRunnables();
  }

  @Override
  public List<Order> getOrders() {
    return delegate.getOrders();
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return delegate.getEventHandler();
  }
}
