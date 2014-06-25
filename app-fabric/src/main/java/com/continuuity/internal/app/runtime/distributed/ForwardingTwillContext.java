package com.continuuity.internal.app.runtime.distributed;

import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.InetAddress;

/**
 * Forwarding Twill Context for use in Service TwillRunnable.
 */
public abstract class ForwardingTwillContext implements TwillContext {
  private final TwillContext delegate;

  protected ForwardingTwillContext(TwillContext context) {
    this.delegate = context;
  }


  @Override
  public RunId getRunId() {
    return delegate.getRunId();
  }

  @Override
  public RunId getApplicationRunId() {
    return delegate.getApplicationRunId();
  }

  @Override
  public int getInstanceCount() {
    return delegate.getInstanceCount();
  }

  @Override
  public InetAddress getHost() {
    return delegate.getHost();
  }

  @Override
  public String[] getArguments() {
    return delegate.getArguments();
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return delegate.getSpecification();
  }

  @Override
  public int getInstanceId() {
    return delegate.getInstanceId();
  }

  @Override
  public int getVirtualCores() {
    return delegate.getVirtualCores();
  }

  @Override
  public int getMaxMemoryMB() {
    return delegate.getMaxMemoryMB();
  }

  @Override
  public ServiceDiscovered discover(String s) {
    return delegate.discover(s);
  }

  @Override
  public Cancellable announce(String s, int i) {
    return delegate.announce(s, i);
  }
}
