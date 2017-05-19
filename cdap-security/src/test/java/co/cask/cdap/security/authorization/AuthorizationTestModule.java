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

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;


/**
 * A {@link PrivateModule} that can be used in tests. Tests can enforce authorization in this module by setting
 * {@link Constants.Security.Authorization#ENABLED} and {@link Constants.Security#ENABLED} to {@code true}. However,
 * this module exposes an {@link AuthorizerInstantiator} whose {@link AuthorizationContextFactory} returns a no-op
 * {@link AuthorizationContext} that cannot perform any {@link DatasetContext}, {@link Admin} or {@link Transactional}
 * operations.
 */
public class AuthorizationTestModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(AuthorizationContextFactory.class).to(NoOpAuthorizationContextFactory.class);
    bind(AuthorizerInstantiator.class).in(Scopes.SINGLETON);
    expose(AuthorizerInstantiator.class);
    bind(PrivilegesManager.class).to(DelegatingPrivilegeManager.class).in(Scopes.SINGLETON);
    expose(PrivilegesManager.class);
  }
}
