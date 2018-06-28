/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A Metadata search request.
 */
public class SearchRequest {
  private final NamespaceId namespace;
  private final String query;
  private final Set<EntityTypeSimpleName> types;
  private final SortInfo sortInfo;
  private final int offset;
  private final int limit;
  private final int numCursors;
  @Nullable
  private final String cursor;
  private final boolean showHidden;
  private final Set<EntityScope> entityScope;

  public SearchRequest(NamespaceId namespace, String query, Set<EntityTypeSimpleName> types, SortInfo sortInfo,
                       int offset, int limit, int numCursors, @Nullable String cursor, boolean showHidden,
                       Set<EntityScope> entityScope) {
    if (offset < 0) {
      throw new IllegalArgumentException("offset must not be negative");
    }
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    this.namespace = namespace;
    this.query = query;
    this.types = Collections.unmodifiableSet(new HashSet<>(types));
    this.sortInfo = sortInfo;
    this.numCursors = numCursors;
    this.cursor = cursor;
    this.showHidden = showHidden;
    this.entityScope = Collections.unmodifiableSet(new HashSet<>(entityScope));
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * @return the namespace to search in
   */
  public NamespaceId getNamespace() {
    return namespace;
  }

  /**
   * The search query can be of two form: [key]:[value] or just [value] and can have '*' at the end for a prefix search.
   *
   * @return the search query
   */
  public String getQuery() {
    return query;
  }

  /**
   * The types of entities to restrict the search to. If empty, all types should be searched.
   *
   * @return the types of entities to restrict the search to
   */
  public Set<EntityTypeSimpleName> getTypes() {
    return types;
  }

  /**
   * @return how to sort the results
   */
  public SortInfo getSortInfo() {
    return sortInfo;
  }

  /**
   * The offset to start with in the search results. {@code 0} means results should be returned from the beginning.
   * Only applies when {@link #getSortInfo()} is not {@link SortInfo#DEFAULT}.
   *
   * @return the offset to start the results at
   */
  public int getOffset() {
    return offset;
  }

  /**
   * The max number of results to return, starting from {@link #getOffset()}. Only applies when {@link #getSortInfo()}
   * is not {@link SortInfo#DEFAULT}.
   *
   * @return the maximum number of results to return
   */
  public int getLimit() {
    return limit;
  }

  /**
   * The number of cursors to return in the response. A cursor identifies the first index of the next page for
   * pagination purposes. Only applies when {@link #getSortInfo()} is not {@link SortInfo#DEFAULT}.
   *
   * @return number of cursors to return in the response
   */
  public int getNumCursors() {
    return numCursors;
  }

  /**
   * The cursor that acts as the starting index for the request page. This is only applicable when
   * {@link #getSortInfo()} is not {@link SortInfo#DEFAULT}. If offset is also specified, it is applied starting at
   * the cursor. If not present, the first row is used as the cursor.
   *
   * @return the cursor for search results, or null if there is no cursor
   */
  @Nullable
  public String getCursor() {
    return cursor;
  }

  /**
   * @return whether to display hidden entities (entities whose name start with '_').
   */
  public boolean shouldShowHidden() {
    return showHidden;
  }

  /**
   * @return scope of entities to display
   */
  public Set<EntityScope> getEntityScope() {
    return entityScope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchRequest that = (SearchRequest) o;
    return offset == that.offset &&
      limit == that.limit &&
      numCursors == that.numCursors &&
      showHidden == that.showHidden &&
      Objects.equals(namespace, that.namespace) &&
      Objects.equals(query, that.query) &&
      Objects.equals(types, that.types) &&
      Objects.equals(sortInfo, that.sortInfo) &&
      Objects.equals(cursor, that.cursor) &&
      Objects.equals(entityScope, that.entityScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, query, types, sortInfo, offset, limit, numCursors, cursor, showHidden, entityScope);
  }

  @Override
  public String toString() {
    return "SearchRequest{" +
      "namespace=" + namespace +
      ", query='" + query + '\'' +
      ", types=" + types +
      ", sortInfo=" + sortInfo +
      ", offset=" + offset +
      ", limit=" + limit +
      ", numCursors=" + numCursors +
      ", cursor='" + cursor + '\'' +
      ", showHidden=" + showHidden +
      ", entityScope=" + entityScope +
      '}';
  }
}
