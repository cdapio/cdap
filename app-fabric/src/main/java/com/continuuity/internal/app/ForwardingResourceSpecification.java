package com.continuuity.internal.app;

import org.apache.twill.api.ResourceSpecification;

import java.util.List;

/**
 * Forwarding ResourceSpecification implementation.
 */
public abstract class ForwardingResourceSpecification implements ResourceSpecification {

  private final ResourceSpecification delegate;

  protected ForwardingResourceSpecification(ResourceSpecification specification) {
    this.delegate = specification;
  }

  @Override
  public int getCores() {
    return delegate.getCores();
  }

  @Override
  public int getVirtualCores() {
    return delegate.getVirtualCores();
  }

  @Override
  public int getMemorySize() {
    return delegate.getMemorySize();
  }

  @Override
  public int getUplink() {
    return delegate.getUplink();
  }

  @Override
  public int getDownlink() {
    return delegate.getDownlink();
  }

  @Override
  public int getInstances() {
    return delegate.getInstances();
  }

  @Override
  public List<String> getHosts() {
    return delegate.getHosts();
  }

  @Override
  public List<String> getRacks() {
    return delegate.getRacks();
  }
}
