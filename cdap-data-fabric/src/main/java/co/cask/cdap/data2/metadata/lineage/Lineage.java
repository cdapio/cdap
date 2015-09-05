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

import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;
import java.util.Set;

/**
 * Represents the data access relations between Programs and Datasets.
 */
public class Lineage {
  private final Set<Relation> relations;
  private final Set<Id.Program> programs;
  private final Set<Id.DatasetInstance> data;

  public Lineage(Set<Relation> relations, Set<Id.Program> programs, Set<Id.DatasetInstance> data) {
    this.relations = ImmutableSet.copyOf(relations);
    this.programs = ImmutableSet.copyOf(programs);
    this.data = ImmutableSet.copyOf(data);
  }

  public Set<Relation> getRelations() {
    return relations;
  }

  public Set<Id.Program> getPrograms() {
    return programs;
  }

  public Set<Id.DatasetInstance> getData() {
    return data;
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
    return Objects.equals(relations, lineage.relations) &&
      Objects.equals(programs, lineage.programs) &&
      Objects.equals(data, lineage.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relations, programs, data);
  }

  @Override
  public String toString() {
    return "Lineage{" +
      "relations=" + relations +
      ", programs=" + programs +
      ", data=" + data +
      '}';
  }
}
