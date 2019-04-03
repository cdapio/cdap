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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.metadata.MetadataScope;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a metadata search request.
 */
@Beta
public class SearchRequest {

  private final String query;
  private final MetadataScope scope;
  private final Set<String> namespaces;
  private final Set<String> types;
  private final boolean showHidden;
  private final int offset;
  private final int limit;
  private final String cursor;
  private final boolean cursorRequested;
  private final Sorting sorting;

  /**
   * Create a new search request.
   *
   * @param query the query string
   * @param scope if non-null, limits the search to this scope
   * @param namespaces if non-null, limits the search to these namespaces
   * @param types if non-null, limits the search to these types of metadata entities
   * @param showHidden whether to return hidden entities (whose names begin with '_')
   * @param offset the offset to start returning results from
   * @param limit limits the number of results to return
   * @param cursor if non-null, a cursor returned by a previous search. A cursor allows the search
   *               engine to fetch the next set of results (starting at offset + limit) more efficiently.
   *               Not all implementation support this optimization.
   * @param cursorRequested whether this search should return a cursor. An implementation that does
   *                        not support cursors will not respect this parameter and never return a cursor.
   * @param sorting if non-null, how to sort the results, otherwise by relevance
   */
  private SearchRequest(String query,
                        @Nullable MetadataScope scope,
                        @Nullable Set<String> namespaces,
                        @Nullable Set<String> types,
                        boolean showHidden,
                        int offset,
                        int limit,
                        @Nullable String cursor,
                        boolean cursorRequested,
                        @Nullable Sorting sorting) {
    this.query = query;
    this.scope = scope;
    this.namespaces = namespaces;
    this.types = types;
    this.showHidden = showHidden;
    this.offset = offset;
    this.limit = limit;
    this.cursor = cursor;
    this.cursorRequested = cursorRequested;
    this.sorting = sorting;
  }

  public String getQuery() {
    return query;
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

  public boolean isShowHidden() {
    return showHidden;
  }

  public int getOffset() {
    return offset;
  }

  public int getLimit() {
    return limit;
  }

  public String getCursor() {
    return cursor;
  }

  public boolean isCursorRequested() {
    return cursorRequested;
  }

  public Sorting getSorting() {
    return sorting;
  }

  public static Builder of(String query) {
    return new Builder(query);
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
    return showHidden == that.showHidden &&
      offset == that.offset &&
      limit == that.limit &&
      cursorRequested == that.cursorRequested &&
      Objects.equals(query, that.query) &&
      scope == that.scope &&
      Objects.equals(namespaces, that.namespaces) &&
      Objects.equals(types, that.types) &&
      Objects.equals(cursor, that.cursor) &&
      Objects.equals(sorting, that.sorting);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, scope, namespaces, types, showHidden, offset, limit, cursor, cursorRequested, sorting);
  }

  @Override
  public String toString() {
    return "SearchRequest{" +
      "query='" + query + '\'' +
      ", scope=" + scope +
      ", namespaces=" + namespaces +
      ", types=" + types +
      ", showHidden=" + showHidden +
      ", offset=" + offset +
      ", limit=" + limit +
      ", cursor='" + cursor + '\'' +
      ", cursorRequested=" + cursorRequested +
      ", sorting=" + sorting +
      '}';
  }

  /**
   * Builder for a search request.
   */
  public static class Builder {

    private final String query;
    private MetadataScope scope;
    private Set<String> namespaces;
    private Set<String> types;
    private boolean showHidden;
    private int offset;
    private int limit = 10;
    private String cursor;
    private boolean cursorRequested;
    private Sorting sorting;

    private Builder(String query) {
      this.query = query;
    }

    public Builder setScope(MetadataScope scope) {
      this.scope = scope;
      return this;
    }

    public Builder addNamespace(String namespace) {
      if (namespaces == null) {
        namespaces = new HashSet<>();
      }
      namespaces.add(namespace);
      return this;
    }

    public Builder addSystemNamespace() {
      return this.addNamespace("system");
    }

    public Builder addType(String type) {
      if (types == null) {
        types = new HashSet<>();
      }
      types.add(type);
      return this;
    }

    public Builder setShowHidden(boolean showHidden) {
      this.showHidden = showHidden;
      return this;
    }

    public Builder setOffset(int offset) {
      this.offset = offset;
      return this;
    }

    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder setCursor(String cursor) {
      this.cursor = cursor;
      return this;
    }

    public Builder setCursorRequested(boolean cursorRequested) {
      this.cursorRequested = cursorRequested;
      return this;
    }

    public Builder setSorting(Sorting sorting) {
      this.sorting = sorting;
      return this;
    }

    public SearchRequest build() {
      return new SearchRequest(query, scope, namespaces, types, showHidden,
                               offset, limit, cursor, cursorRequested, sorting);
    }
  }
}
