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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.metadata.MetadataScope;

import java.util.Objects;

/**
 * Identifies a named piece of metadata of a specific kind in a given scope,
 * for example, property "schema" in scope SYSTEM, or tag "finance" in scope USER.
 */
@Beta
public class ScopedNameOfKind extends ScopedName {
  private final MetadataKind kind;

  public ScopedNameOfKind(MetadataKind kind, MetadataScope scope, String name) {
    super(scope, name);
    this.kind = kind;
  }

  public ScopedNameOfKind(MetadataKind kind, ScopedName scopedName) {
    this(kind, scopedName.getScope(), scopedName.getName());
  }

  public MetadataKind getKind() {
    return kind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ScopedNameOfKind that = (ScopedNameOfKind) o;
    return kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), kind);
  }

  @Override
  public String toString() {
    return '(' + kind.name().toLowerCase() + ')' + super.toString();
  }

  public static ScopedNameOfKind fromString(String s) {
    // Based on ScopedNameOfKind string format, the kind must be in parenthesis, followed by the ScopedName
    if (s.indexOf("(") != 0 || s.indexOf(")") <= 1) {
      throw new IllegalArgumentException(String.format("Cannot parse '%s' as a ScopedNameOfKind", s));
    }
    MetadataKind kind = MetadataKind.valueOf(s.substring(s.indexOf("(") + 1, s.indexOf(")")).toUpperCase());
    ScopedName scopedName = ScopedName.fromString(s.substring(s.indexOf(")") + 1));
    return new ScopedNameOfKind(kind, scopedName);
  }
}
