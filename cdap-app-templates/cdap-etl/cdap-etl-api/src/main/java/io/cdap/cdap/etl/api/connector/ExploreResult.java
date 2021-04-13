/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.connector;

import java.util.List;
import java.util.Objects;

/**
 * The explore result for the given request
 */
public class ExploreResult {
  // this count represents the total count of entities, when pagination is added in the future,
  // this count might not be equal to entities.size()
  private final int count;
  private final List<ExploreEntity> entities;

  public ExploreResult(int count, List<ExploreEntity> entities) {
    this.count = count;
    this.entities = entities;
  }

  public int getCount() {
    return count;
  }

  public List<ExploreEntity> getEntities() {
    return entities;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExploreResult that = (ExploreResult) o;
    return count == that.count &&
      Objects.equals(entities, that.entities);
  }

  @Override
  public int hashCode() {
    return Objects.hash(count, entities);
  }
}
