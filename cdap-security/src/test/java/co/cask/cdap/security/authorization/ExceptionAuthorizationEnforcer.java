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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;

import java.util.Set;

/**
 * A dummy AuthorizationEnforcer which throws {@link ExpectedException} when enforce is called.
 * This is used for testing as in {@link AuthEnforceRewriterTest} to ensure that enforce was successfully called after
 * class rewrite.
 */
public class ExceptionAuthorizationEnforcer implements AuthorizationEnforcer {

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    throw new ExpectedException(entity);
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    throw new ExpectedException(entity);
  }

  @Override
  public Predicate<EntityId> createFilter(Principal principal) throws Exception {
    return null;
  }

  class ExpectedException extends Exception {
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
