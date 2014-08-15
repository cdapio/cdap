/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import org.apache.twill.api.ElectionHandler;
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
  public String[] getApplicationArguments() {
    return delegate.getApplicationArguments();
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
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    return delegate.electLeader(name, participantHandler);
  }

  @Override
  public Cancellable announce(String s, int i) {
    return delegate.announce(s, i);
  }
}
