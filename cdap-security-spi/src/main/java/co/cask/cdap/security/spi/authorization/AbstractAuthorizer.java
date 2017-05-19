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

package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;

/**
 * Abstract class that implements {@link Authorizer} and provides default no-op implementations of
 * {@link Authorizer#initialize(AuthorizationContext)} and {@link Authorizer#destroy()} so classes extending it do not
 * have to implement these methods unless necessary.
 */
public abstract class AbstractAuthorizer implements Authorizer {

  protected static final Predicate<EntityId> ALLOW_ALL = new Predicate<EntityId>() {
    @Override
    public boolean apply(EntityId entityId) {
      return true;
    }
  };

  /**
   * Default no-op implementation of {@link Authorizer#initialize(AuthorizationContext)}.
   */
  @Override
  public void initialize(AuthorizationContext context) throws Exception {
    // default no-op implementation
  }

  /**
   * Default no-op implementation of {@link Authorizer#destroy()}.
   */
  @Override
  public void destroy() throws Exception {
    // default no-op implementation
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
    enforce(entity, principal, Collections.singleton(action));
  }

  @Override
  public Predicate<EntityId> createFilter(final Principal principal) throws Exception {
    return new Predicate<EntityId>() {
      @Override
      public boolean apply(EntityId entityId) {
        try {
          enforce(entityId, principal, EnumSet.allOf(Action.class));
          return true;
        } catch (UnauthorizedException e) {
          return false;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }
}
