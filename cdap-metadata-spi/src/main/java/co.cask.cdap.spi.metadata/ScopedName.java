/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.spi.metadata;

import co.cask.cdap.api.metadata.MetadataScope;

import java.util.Objects;

/**
 * Identifies a datum (a tag, or property) within a metadata scope.
 */
public class ScopedName {
  private final MetadataScope scope;
  private final String name;

  public ScopedName(MetadataScope scope, String name) {
    this.scope = scope;
    this.name = name;
  }

  public MetadataScope getScope() {
    return scope;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScopedName other = (ScopedName) o;
    return scope == other.scope &&
      Objects.equals(name, other.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, name);
  }

  @Override
  public String toString() {
    return scope.name() + ':' + name;
  }

  public static ScopedName fromString(String s) {
    String[] parts = s.split(":", 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException(String.format("Cannot parse '%s' as a ScopedName", s));
    }
    MetadataScope scope = MetadataScope.valueOf(parts[0]);
    return new ScopedName(scope, parts[1]);
  }
}
