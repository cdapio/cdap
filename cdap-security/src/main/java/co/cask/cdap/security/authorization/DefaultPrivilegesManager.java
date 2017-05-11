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

package co.cask.cdap.security.authorization;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.inject.Inject;

import java.util.Set;

/**
 * A {@link PrivilegesManager} that also invalidates privileges caches when privileges are updated.
 */
public class DefaultPrivilegesManager implements PrivilegesManager {

  private final Authorizer delegateAuthorizer;

  @Inject
  DefaultPrivilegesManager(AuthorizerInstantiator authorizerInstantiator) {
    this.delegateAuthorizer = authorizerInstantiator.get();
  }

  @Override
  public void grant(EntityId entity, final Principal principal, Set<Action> actions) throws Exception {
    delegateAuthorizer.grant(entity, principal, actions);

  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    delegateAuthorizer.revoke(entity, principal, actions);
  }

  @Override
  public void revoke(EntityId entity) throws Exception {
    delegateAuthorizer.revoke(entity);
  }

  @Override
  public Set<Privilege> listPrivileges(Principal principal) throws Exception {
    return delegateAuthorizer.listPrivileges(principal);
  }
}
