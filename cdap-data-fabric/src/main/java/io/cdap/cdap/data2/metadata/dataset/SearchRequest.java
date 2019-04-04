/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.dataset;

import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A Metadata search request.
 */
public class SearchRequest {
  private final NamespaceId namespaceId;
  private final String query;
  private final Set<String> types;
  private final SortInfo sortInfo;
  private final int offset;
  private final int limit;
  private final int numCursors;
  @Nullable
  private final String cursor;
  private final boolean showHidden;
  private final Set<EntityScope> entityScope;

  /**
   * Represents a request for a search for CDAP entities in the specified namespace with the specified search query and
   * an optional set of entity types in the specified {@link MetadataScope}.
   *
   * @param namespaceId the namespace id to filter the search by. Null if the search is across all namespaces
   * @param query the search query
   * @param types the types of CDAP entity to be searched. If empty all possible types will be searched
   * @param sortInfo represents sorting information. Use {@link SortInfo#DEFAULT} to return search results without
   *                 sorting (which implies that the sort order is by relevance to the search query)
   * @param offset the index to start with in the search results. To return results from the beginning, pass {@code 0}
   * @param limit the number of results to return, starting from #offset. To return all, pass {@link Integer#MAX_VALUE}
   * @param numCursors the number of cursors to return in the response. A cursor identifies the first index of the
   *                   next page for pagination purposes. Defaults to {@code 0}
   * @param cursor the cursor that acts as the starting index for the requested page. This is only applicable when
   *               #sortInfo is not {@link SortInfo#DEFAULT}. If offset is also specified, it is applied starting at
   *               the cursor. If {@code null}, the first row is used as the cursor
   * @param showHidden boolean which specifies whether to display hidden entities (entity whose name start with "_")
   *                    or not.
   * @param entityScope a set which specifies which scope of entities to display.
   */
  public SearchRequest(@Nullable NamespaceId namespaceId, String query, Set<String> types,
                       SortInfo sortInfo, int offset, int limit, int numCursors, @Nullable String cursor,
                       boolean showHidden, Set<EntityScope> entityScope) {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("query must be specified");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("offset must not be negative");
    }
    if (limit < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    if (numCursors < 0) {
      throw new IllegalArgumentException("numCursors must not be negative");
    }
    if (entityScope.isEmpty()) {
      throw new IllegalArgumentException("entity scope must be specified");
    }
    this.namespaceId = namespaceId;
    this.query = query;
    this.types = types.stream().map(String::toLowerCase).collect(Collectors.toSet());
    this.sortInfo = sortInfo;
    this.numCursors = numCursors;
    this.cursor = cursor;
    this.showHidden = showHidden;
    this.entityScope = Collections.unmodifiableSet(new HashSet<>(entityScope));
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * @return the namespace to search in, or empty if this is a cross namespace search.
   */
  public Optional<NamespaceId> getNamespaceId() {
    return Optional.ofNullable(namespaceId);
  }

  /**
   * @return true if the search request is within a namespace, false if it is across namespaces.
   */
  public boolean isNamespaced() {
    return namespaceId != null;
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
  public Set<String> getTypes() {
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
   *   Hidden entities are system entities like streams, dataset, programs for system applications like tracker.
   */
  public boolean shouldShowHidden() {
    return showHidden;
  }

  /**
   * @return whether the search is done on the system metadata table or user metadata table.
   *   If a scope not defined the scan is performed on both and results are aggregated .
   */
  public Set<EntityScope> getEntityScopes() {
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
      Objects.equals(namespaceId, that.namespaceId) &&
      Objects.equals(query, that.query) &&
      Objects.equals(types, that.types) &&
      Objects.equals(sortInfo, that.sortInfo) &&
      Objects.equals(cursor, that.cursor) &&
      Objects.equals(entityScope, that.entityScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespaceId, query, types, sortInfo, offset, limit, numCursors, cursor, showHidden,
                        entityScope);
  }

  @Override
  public String toString() {
    return "SearchRequest{" +
      "namespaceId=" + namespaceId +
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
