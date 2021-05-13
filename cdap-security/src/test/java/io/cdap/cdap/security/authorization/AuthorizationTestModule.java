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

package io.cdap.cdap.security.authorization;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.PermissionManager;


/**
 * A {@link PrivateModule} that can be used in tests. Tests can enforce authorization in this module by setting
 * {@link Constants.Security.Authorization#ENABLED} and {@link Constants.Security#ENABLED} to {@code true}. However,
 * this module exposes an {@link AccessControllerInstantiator} whose {@link AuthorizationContextFactory} returns a no-op
 * {@link AuthorizationContext} that cannot perform any {@link DatasetContext}, {@link Admin} or {@link Transactional}
 * operations.
 */
public class AuthorizationTestModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(AuthorizationContextFactory.class).to(NoOpAuthorizationContextFactory.class);
    bind(AccessControllerInstantiator.class).in(Scopes.SINGLETON);
    expose(AccessControllerInstantiator.class);
    bind(PermissionManager.class).to(DelegatingPermissionManager.class).in(Scopes.SINGLETON);
    expose(PermissionManager.class);
  }
}
