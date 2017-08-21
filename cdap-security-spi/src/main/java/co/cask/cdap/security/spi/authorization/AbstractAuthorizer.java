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
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

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
    throw new UnsupportedOperationException("createFilter() is deprecated, please use isVisible() instead");
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    // grant on EntityId is deprecated 4.3 onwards. This implementation is added to remove the burden from the
    // underlying extension to provide an implementation. This API will be removed in 4.4
    grant(Authorizable.fromEntityId(entity), principal, actions);
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    // revoke on EntityId is deprecated 4.3 onwards. This implementation is added to remove the burden from the
    // underlying extension to provide an implementation. This API will be removed in 4.4
    revoke(Authorizable.fromEntityId(entity), principal, actions);
  }

  @Override
  public void revoke(EntityId entity) throws Exception {
    // revoke on EntityId is deprecated 4.3 onwards. This implementation is added to remove the burden from the
    // underlying extension to provide an implementation. This API will be removed in 4.4
    revoke(Authorizable.fromEntityId(entity));
  }
}
