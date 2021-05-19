/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.PermissionManager;

import java.util.Set;

/**
 * A {@link PermissionManager} implements that delegates to the authorizer.
 * Having this makes Guice injection for Privilege manager simple. That reason will go away once
 * https://issues.cask.co/browse/CDAP-11561 is fixed.
 */
public class DelegatingPermissionManager implements PermissionManager {

  private final AccessController delegateAuthorizer;

  @Inject
  DelegatingPermissionManager(AccessControllerInstantiator accessControllerInstantiator) {
    this.delegateAuthorizer = accessControllerInstantiator.get();
  }

  @Override
  public void grant(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    delegateAuthorizer.grant(authorizable, principal, permissions);
  }

  @Override
  public void revoke(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions)
    throws AccessException {
    delegateAuthorizer.revoke(authorizable, principal, permissions);
  }

  @Override
  public void revoke(Authorizable authorizable) throws AccessException {
    delegateAuthorizer.revoke(authorizable);
  }

  @Override
  public Set<GrantedPermission> listGrants(Principal principal) throws AccessException {
    return delegateAuthorizer.listGrants(principal);
  }
}
