/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.entity;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import javax.annotation.Nullable;

/**
 * Class for holding paginated search result entities
 * @param <T> The type of entities
 */
public class EntityResult<T> {

  private final Collection<? extends T> entities;
  private final String cursor;
  private final int offset;
  private final int limit;
  private final int totalResults;

  public EntityResult(Collection<? extends T> entities, @Nullable String cursor, int offset, int limit,
                      int totalResults) {
    this.entities = Collections.unmodifiableSet(new LinkedHashSet<>(entities));
    this.cursor = cursor;
    this.offset = offset;
    this.limit = limit;
    this.totalResults = totalResults;
  }

  /**
   * @return {@link Collection} of entities
   */
  public Collection<? extends T> getEntities() {
    return entities;
  }

  /**
   * @return {@link String} cursor from search result if supported by backend
   */
  @Nullable
  public String getCursor() {
    return cursor;
  }

  /**
   * @return int offset that was used in the search
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return int limit that was used in the search
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return int with total number of results or an estimate
   */
  public int getTotalResults() {
    return totalResults;
  }
}
