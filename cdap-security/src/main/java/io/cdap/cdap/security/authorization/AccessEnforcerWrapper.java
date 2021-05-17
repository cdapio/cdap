/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.AccessException;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.Authorizer;

import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * Wraps an {@link io.cdap.cdap.security.spi.authorization.AccessEnforcer} and makes {@link AuthorizationEnforcer}
 * out of it.
 * TODO: remove after platform fully migrated to use AccessController directly
 */
public class AccessEnforcerWrapper implements AuthorizationEnforcer {
  private final AccessEnforcer accessEnforcer;

  @Inject
  public AccessEnforcerWrapper(AccessEnforcer accessEnforcer) {
    this.accessEnforcer = accessEnforcer;
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws AccessException {
    accessEnforcer.enforce(entity, principal, action.getPermission());
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    accessEnforcer.enforce(entity, principal, getPermissions(actions));
  }


  @Override
  public void isVisible(EntityId entityId, Principal principal) throws AccessException {
    accessEnforcer.isVisible(entityId, principal);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
    throws AccessException {
    return accessEnforcer.isVisible(entityIds, principal);
  }

  protected Set<Permission> getPermissions(Set<Action> actions) {
    return actions.stream().map(Action::getPermission).collect(Collectors.toSet());
  }

}
