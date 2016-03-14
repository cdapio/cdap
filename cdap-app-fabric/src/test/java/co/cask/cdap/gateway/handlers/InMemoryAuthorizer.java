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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * In-memory implementation of {@link Authorizer}.
 */
@NotThreadSafe
public class InMemoryAuthorizer implements Authorizer {

  private final Table<EntityId, Principal, Set<Action>> table = HashBasedTable.create();

  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws UnauthorizedException {
    Set<Action> allowed = get(entity, principal);
    if (!(allowed.contains(Action.ALL) || allowed.contains(action))) {
      throw new UnauthorizedException(principal, action, entity);
    }
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) {
    get(entity, principal).addAll(actions);
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) {
    get(entity, principal).removeAll(actions);
  }

  @Override
  public void revoke(EntityId entity) {
    for (Principal principal : table.row(entity).keySet()) {
      get(entity, principal).clear();
    }
  }

  private Set<Action> get(EntityId entity, Principal principal) {
    if (!table.contains(entity, principal)) {
      table.put(entity, principal, new HashSet<Action>());
    }
    return table.get(entity, principal);
  }
}
