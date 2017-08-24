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

package co.cask.cdap.proto.security;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.proto.id.EntityId;

import java.util.Objects;

/**
 * Represents a privilege granted to a {@link Principal user}, {@link Principal group} or a {@link Principal role}.
 * It determines if the user or group can perform a given {@link Action} on an {@link EntityId}.
 */
@Beta
public class Privilege {
  private final Authorizable authorizable;
  private final Action action;

  public Privilege(EntityId entityId, Action action) {
    this(Authorizable.fromEntityId(entityId), action);
  }

  public Privilege(Authorizable authorizable, Action action) {
    this.authorizable = authorizable;
    this.action = action;
  }

  public Authorizable getAuthorizable() {
    return authorizable;
  }

  public Action getAction() {
    return action;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Privilege)) {
      return false;
    }

    Privilege privilege = (Privilege) o;
    return Objects.equals(authorizable, privilege.authorizable) && Objects.equals(action, privilege.action);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authorizable, action);
  }

  @Override
  public String toString() {
    return "Privilege{" +
      "authorizable=" + authorizable +
      ", action=" + action +
      '}';
  }
}
