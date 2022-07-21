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

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;

import java.util.Collections;
import java.util.Set;

/**
 * A dummy AuthorizationEnforcer which throws {@link ExpectedException} when enforce is called.
 * This is used for testing as in {@link AuthEnforceRewriterTest} to ensure that enforce was successfully called after
 * class rewrite.
 */
public class ExceptionAccessEnforcer implements AccessEnforcer {

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
    throws ExpectedException {
    throw new ExpectedException(entity);
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission)
    throws AccessException {
    throw new ExpectedException(parentId);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
    return Collections.emptySet();
  }

  class ExpectedException extends AccessException {
    // just a dummy exception for test which is thrown if authorization enforcement call was successful
    private final EntityId entityId; // entity on which authorization enforcement is being performed

    ExpectedException(EntityId entityId) {
      this.entityId = entityId;
    }

    public EntityId getEntityId() {
      return entityId;
    }
  }
}
