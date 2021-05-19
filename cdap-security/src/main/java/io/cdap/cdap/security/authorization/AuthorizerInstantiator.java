/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.security.spi.authorization.Authorizer;

import java.util.function.Supplier;
import javax.inject.Inject;

/**
 * Temporary gateway for old platform classes to get {@link Authorizer} until after migration
 * to {@link io.cdap.cdap.security.spi.authorization.AccessController} is finished.
 */
public class AuthorizerInstantiator implements Supplier<Authorizer> {
  private final AccessControllerInstantiator accessControllerInstantiator;

  @Inject
  public AuthorizerInstantiator(AccessControllerInstantiator accessControllerInstantiator) {
    this.accessControllerInstantiator = accessControllerInstantiator;
  }

  @VisibleForTesting
  public AuthorizerInstantiator(CConfiguration cConf, AuthorizationContextFactory authorizationContextFactory) {
    this(new AccessControllerInstantiator(cConf, authorizationContextFactory));
  }

  @Override
  public Authorizer get() {
    return new AccessControllerWrapper(accessControllerInstantiator.get());
  }
}
