/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

import java.util.concurrent.Executor;

/**
 * This class acts as a wrapper for {@link AuthorizerInstantiatorService}. Methods {@link #start}
 * create new instance of the {@link AuthorizerInstantiatorService}, so that even the instance
 * {@link AuthorizerSupplier} is injected as singleton we can control the start up
 * of underlying {@link AuthorizerInstantiatorService}.
 */
public class AuthorizerSupplier implements Service, Supplier<Authorizer> {

  private final CConfiguration cConf;
  private final AuthorizationContextFactory authorizationContextFactory;

  private AuthorizerInstantiatorService delegate;

  @Inject
  @VisibleForTesting
  public AuthorizerSupplier(CConfiguration cConf, AuthorizationContextFactory authorizationContextFactory) {
    this.cConf = cConf;
    this.authorizationContextFactory = authorizationContextFactory;
  }

  /**
   * Returns an instance of the configured {@link Authorizer} extension, or of {@link NoOpAuthorizer}, if
   * authorization is disabled.
   */
  @Override
  public Authorizer get() {
    Preconditions.checkState(delegate != null && delegate.isRunning(),
                             "Authorization Service has not yet started. Authorizer not available.");

    return delegate.getAuthorizer();
  }

  @Override
  public ListenableFuture<State> start() {
    delegate = new AuthorizerInstantiatorService(cConf, authorizationContextFactory);
    return delegate.start();
  }

  @Override
  public State startAndWait() {
    delegate = new AuthorizerInstantiatorService(cConf, authorizationContextFactory);
    return delegate.startAndWait();
  }

  @Override
  public boolean isRunning() {
    return delegate.isRunning();
  }

  @Override
  public State state() {
    return delegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return delegate.stop();
  }

  @Override
  public State stopAndWait() {
    return delegate.stopAndWait();
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    // No-op
  }
}
