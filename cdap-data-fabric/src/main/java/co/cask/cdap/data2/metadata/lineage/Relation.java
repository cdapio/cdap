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
import org.apache.twill.api.RunId;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a Dataset access by a Program.
 */
public class Relation {
  private final Id.DatasetInstance data;
  private final Id.Program program;
  private final AccessType access;
  private final Set<RunId> runs;
  private final Set<Id.NamespacedId> components;

  public Relation(Id.DatasetInstance data, Id.Program program, AccessType access, Set<RunId> runs,
                  Set<? extends Id.NamespacedId> components) {
    this.data = data;
    this.program = program;
    this.access = access;
    this.runs = ImmutableSet.copyOf(runs);
    this.components = ImmutableSet.copyOf(components);
  }

  public Id.DatasetInstance getData() {
    return data;
  }

  public Id.Program getProgram() {
    return program;
  }

  public AccessType getAccess() {
    return access;
  }

  public Set<RunId> getRuns() {
    return runs;
  }

  public Set<Id.NamespacedId> getComponents() {
    return components;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Relation relation = (Relation) o;
    return Objects.equals(data, relation.data) &&
      Objects.equals(program, relation.program) &&
      Objects.equals(access, relation.access) &&
      Objects.equals(runs, relation.runs) &&
      Objects.equals(components, relation.components);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, access, runs, components);
  }

  @Override
  public String toString() {
    return "Relation{" +
      "data=" + data +
      ", program=" + program +
      ", access=" + access +
      ", runs=" + runs +
      ", components=" + components +
      '}';
  }
}
