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

package co.cask.cdap.data2.metadata.lineage;

import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Represents the data access relations between Programs and Datasets.
 */
public class Lineage {
  private final Set<Relation> relations;

  public Lineage(Iterable<? extends Relation> relations) {
    this.relations = ImmutableSet.copyOf(relations);
  }

  public Set<Relation> getRelations() {
    return relations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Lineage lineage = (Lineage) o;
    return Objects.equals(relations, lineage.relations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relations);
  }

  @Override
  public String toString() {
    return "Lineage{" +
      "relations=" + relations +
      '}';
  }
}
