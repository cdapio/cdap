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

/**
 * This class acts as a wrapper for {@link AuthorizationService}. Methods {@link #start}
 * create new instance of the {@link AuthorizationService}, so that even the instance
 * {@link AuthorizerInstantiatorService} is injected as singleton we can control the start up
 * of underlying {@link AuthorizationService}.
 */
public class AuthorizerInstantiatorService implements Supplier<Authorizer> {

  private final CConfiguration cConf;
  private final AuthorizationContextFactory authorizationContextFactory;

  private AuthorizationService delegate;

  @Inject
  @VisibleForTesting
  public AuthorizerInstantiatorService(CConfiguration cConf, AuthorizationContextFactory authorizationContextFactory) {
    this.cConf = cConf;
    this.authorizationContextFactory = authorizationContextFactory;
  }

  /**
   * Initiates the startup of the underlying {@link AuthorizationService}.
   */
  public ListenableFuture<Service.State> start() {
    delegate = new AuthorizationService(cConf, authorizationContextFactory);
    return delegate.start();
  }

  /**
   * Initiates the startup of the underlying {@link AuthorizationService}.
   */
  public Service.State startAndWait() {
    delegate = new AuthorizationService(cConf, authorizationContextFactory);
    return delegate.startAndWait();
  }

  /**
   * Initiates the underlying service shutdown.
   */
  public Service.State stopAndWait() {
    return delegate.stopAndWait();
  }

  /**
   * Returns true if the underlying {@link AuthorizationService} is running, false otherwise.
   */
  public boolean isRunning() {
    return delegate.isRunning();
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
}
