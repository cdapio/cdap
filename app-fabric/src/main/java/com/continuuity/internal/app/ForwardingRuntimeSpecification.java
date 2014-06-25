package com.continuuity.internal.app;

import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnableSpecification;

import java.util.Collection;

/**
 * ForwardingRuntimeSpecification implementation.
 */
public abstract class ForwardingRuntimeSpecification implements RuntimeSpecification {

  private final RuntimeSpecification delegate;

  protected ForwardingRuntimeSpecification(RuntimeSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public TwillRunnableSpecification getRunnableSpecification() {
    return delegate.getRunnableSpecification();
  }

  @Override
  public ResourceSpecification getResourceSpecification() {
    return delegate.getResourceSpecification();
  }

  @Override
  public Collection<LocalFile> getLocalFiles() {
    return delegate.getLocalFiles();
  }
}
