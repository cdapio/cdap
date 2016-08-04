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

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.inject.Inject;

import java.util.Set;

/**
 * A {@link PrivilegesManager} that simply delegates to the configured {@link Authorizer}.
 */
public class AuthorizerAsPrivilegesManager implements PrivilegesManager {

  private final Authorizer authorizer;

  @Inject
  AuthorizerAsPrivilegesManager(AuthorizerInstantiator authorizerInstantiator) {
    this.authorizer = authorizerInstantiator.get();
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    authorizer.grant(entity, principal, actions);
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
    authorizer.revoke(entity, principal, actions);
  }

  @Override
  public void revoke(EntityId entity) throws Exception {
    authorizer.revoke(entity);
  }
}
