package com.continuuity.internal.service;

import com.continuuity.api.service.ServiceSpecification;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class defines a specification for a {@link com.continuuity.api.service.ServiceSpecification}.
 */
public class DefaultServiceSpecification implements ServiceSpecification {

  private final TwillSpecification specification;

  public DefaultServiceSpecification(TwillSpecification specification) {
    this.specification = specification;
  }

  @Override
  public String getClassName() {
    return specification.getClass().getSimpleName();
  }

  @Override
  public String getName() {
    return specification.getName();
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return specification.getRunnables();
  }

  @Override
  public List<Order> getOrders() {
    return specification.getOrders();
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return specification.getEventHandler();
  }

  @Override
  public String getDescription() {
    return "";
  }
}
