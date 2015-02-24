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
package co.cask.common.authorization;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Represents an ID with an associated type.
 */
public class TypedId {

  private final String id;
  private final String type;

  public TypedId(String type, String id) {
    Preconditions.checkNotNull("type cannot be null", type);
    Preconditions.checkNotNull("id cannot be null", id);
    this.type = type;
    this.id = id;
  }

  public static TypedId fromRep(String rep) {
    String[] tokens = rep.split(":");
    if (tokens.length == 0) {
      throw new IllegalArgumentException("Invalid rep format: " + rep);
    }
    return new TypedId(tokens[0], tokens.length <= 1 ? "" : tokens[1]);
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return this.id;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || !(obj instanceof TypedId)) {
      return false;
    }
    final TypedId other = (TypedId) obj;
    return Objects.equal(this.id, other.id) &&
      Objects.equal(this.type, other.type);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("type", type)
      .add("id", id)
      .toString();
  }
}
