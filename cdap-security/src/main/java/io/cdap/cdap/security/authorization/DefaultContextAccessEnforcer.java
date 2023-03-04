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

import com.google.inject.Singleton;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import java.util.Set;
import javax.inject.Inject;

/**
 * An implementation of {@link ContextAccessEnforcer} that retrieves current principal using {@link
 * AuthenticationContext} and delegates to {@link AccessEnforcer}
 */
@Singleton
public class DefaultContextAccessEnforcer implements ContextAccessEnforcer {

  private final AuthenticationContext authenticationContext;
  private final AccessEnforcer accessEnforcer;

  @Inject
  public DefaultContextAccessEnforcer(AuthenticationContext authenticationContext,
      AccessEnforcer accessEnforcer) {
    this.authenticationContext = authenticationContext;
    this.accessEnforcer = accessEnforcer;
  }

  @Override
  public void enforce(EntityId entity, Permission permission) throws AccessException {
    accessEnforcer.enforce(entity, authenticationContext.getPrincipal(), permission);
  }

  @Override
  public void enforce(EntityId entity, Set<? extends Permission> permissions)
      throws AccessException {
    accessEnforcer.enforce(entity, authenticationContext.getPrincipal(), permissions);
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Permission permission)
      throws AccessException {
    accessEnforcer.enforceOnParent(entityType, parentId, authenticationContext.getPrincipal(),
        permission);

  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds)
      throws AccessException {
    return accessEnforcer.isVisible(entityIds, authenticationContext.getPrincipal());
  }
}
