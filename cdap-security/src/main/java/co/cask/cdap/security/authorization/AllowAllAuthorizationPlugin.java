/*
 * Copyright © 2015 Cask Data, Inc.
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

import java.util.Set;

/**
 * {@link AuthorizationPlugin} that allows all access.
 */
public class AllowAllAuthorizationPlugin implements AuthorizationPlugin {

  @Override
  public boolean authorized(EntityId id, String user, Set<Action> actions) {
    return true;
  }

  @Override
  public void grant(EntityId id, String user, Set<Action> actions) {

  }

  @Override
  public void grant(EntityId entity, String user) {

  }

  @Override
  public void revoke(EntityId id, String user, Set<Action> actions) {

  }

  @Override
  public void revoke(EntityId entity, String user) {

  }

  @Override
  public void revoke(EntityId entity) {

  }
}
