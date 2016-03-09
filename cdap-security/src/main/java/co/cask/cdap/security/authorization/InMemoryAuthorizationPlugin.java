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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * In-memory implementation of {@link AuthorizationPlugin}.
 */
@NotThreadSafe
public class InMemoryAuthorizationPlugin implements AuthorizationPlugin {

  private Table<EntityId, String, Set<Action>> table = HashBasedTable.create();

  @Override
  public boolean authorized(EntityId entity, String user, Set<Action> actions) {
    Set<Action> allowed = get(entity, user);
    return allowed.contains(Action.ALL) || allowed.containsAll(actions);
  }

  @Override
  public void grant(EntityId entity, String user, Set<Action> actions) {
    get(entity, user).addAll(actions);
  }

  @Override
  public void grant(EntityId entity, String user) {
    get(entity, user).add(Action.ALL);
  }

  @Override
  public void revoke(EntityId entity, String user, Set<Action> actions) {
    get(entity, user).removeAll(actions);
  }

  @Override
  public void revoke(EntityId entity, String user) {
    get(entity, user).clear();
  }

  @Override
  public void revoke(EntityId entity) {
    for (String user : table.row(entity).keySet()) {
      get(entity, user).clear();
    }
  }

  private Set<Action> get(EntityId entity, String user) {
    if (!table.contains(entity, user)) {
      table.put(entity, user, new HashSet<Action>());
    }
    return table.get(entity, user);
  }
}
