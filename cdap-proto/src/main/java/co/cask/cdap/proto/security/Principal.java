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

/**
 * Identifies a user, group or role which can be authorized to perform an {@link Action} on a {@link EntityId}.
 */
@Beta
public class Principal {
  /**
   * Identifies the type of {@link Principal}.
   */
  public enum PrincipalType {
    USER,
    GROUP,
    ROLE
  }

  private final String name;
  private final PrincipalType type;

  public Principal(String name, PrincipalType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public PrincipalType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Principal principal = (Principal) o;

    return name.equals(principal.name) && type == principal.type;

  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + type.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Principal{" +
      "name='" + name + '\'' +
      ", type=" + type +
      '}';
  }
}
