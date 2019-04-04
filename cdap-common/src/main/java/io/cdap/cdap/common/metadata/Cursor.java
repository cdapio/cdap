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

package io.cdap.cdap.common.metadata;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metadata.MetadataScope;

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A cursor represents the continuation of an existing search query. It therefore encapsulates
 * all the search options, such as offset and limit, sorting, namespaces, etc.
 */
public class Cursor {
  private final int offset;
  private final int limit;
  private final boolean showHidden;
  private final MetadataScope scope;
  private final Set<String> namespaces;
  private final Set<String> types;
  private final String sorting;
  private final String actualCursor;
  private final String query;

  /**
   * Constructor from all components.
   *
   * The only parameter that may contain the ':' character is the query itself. If
   * other parameters contain a ':', then the {@link #fromString(String)} is not
   * expected to work properly.
   */
  public Cursor(int offset, int limit, boolean showHidden,
                @Nullable MetadataScope scope,
                @Nullable Set<String> namespaces,
                @Nullable Set<String> types,
                @Nullable String sorting,
                String actualCursor, String query) {
    this.offset = offset;
    this.limit = limit;
    this.showHidden = showHidden;
    this.scope = scope;
    this.namespaces = namespaces == null || namespaces.isEmpty() ? null : namespaces;
    this.types = types == null || types.isEmpty() ? null : types;
    this.sorting = sorting == null || sorting.isEmpty() ? null : sorting;
    this.actualCursor = actualCursor;
    this.query = query;
  }

  /**
   * Constructor that inherits all parameters from an existing cursor, except the offset and actual cursor.
   */
  public Cursor(Cursor other, int newOffset, String newActual) {
    this(newOffset, other.limit, other.showHidden,
         other.scope, other.namespaces, other.types,
         other.sorting, newActual, other.query);
  }

  public int getOffset() {
    return offset;
  }

  public int getLimit() {
    return limit;
  }

  public boolean isShowHidden() {
    return showHidden;
  }

  public MetadataScope getScope() {
    return scope;
  }

  public Set<String> getNamespaces() {
    return namespaces;
  }

  public Set<String> getTypes() {
    return types;
  }

  public String getSorting() {
    return sorting;
  }

  public String getActualCursor() {
    return actualCursor;
  }

  public String getQuery() {
    return query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Cursor cursor = (Cursor) o;
    return offset == cursor.offset &&
      limit == cursor.limit &&
      showHidden == cursor.showHidden &&
      scope == cursor.scope &&
      Objects.equals(namespaces, cursor.namespaces) &&
      Objects.equals(types, cursor.types) &&
      Objects.equals(sorting, cursor.sorting) &&
      Objects.equals(actualCursor, cursor.actualCursor) &&
      Objects.equals(query, cursor.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, limit, showHidden, scope, namespaces, types, sorting, actualCursor, query);
  }

  @Override
  public String toString() {
    return String.format("%s:%s:%s:%s:%s:%s:%s:%s:%s",
                         offset, limit, showHidden,
                         scope == null ? "" : scope.name(),
                         namespaces == null ? "" : Joiner.on(",").join(namespaces),
                         types == null ? "" : Joiner.on(",").join(types),
                         sorting == null ? "" : sorting,
                         actualCursor,
                         query);
  }

  public static Cursor fromString(String str) {
    String[] parts = str.split(":", 9);
    if (parts.length != 9) {
      throw new IllegalArgumentException("Cursor must have exactly 9 components, but has only " + parts.length);
    }
    int offset = Integer.parseInt(parts[0]);
    int limit = Integer.parseInt(parts[1]);
    boolean showHidden = Boolean.parseBoolean(parts[2]);
    MetadataScope scope = parts[3].isEmpty() ? null : MetadataScope.valueOf(parts[3]);
    Set<String> namespaces = parts[4].isEmpty() ? null : ImmutableSet.copyOf(parts[4].split(","));
    Set<String> types = parts[5].isEmpty() ? null : ImmutableSet.copyOf(parts[5].split(","));
    String sorting = parts[6].isEmpty() ? null : parts[6];
    String actual = parts[7];
    String query = parts[8];

    return new Cursor(offset, limit, showHidden, scope, namespaces, types, sorting, actual, query);
  }
}
