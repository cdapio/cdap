/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.lineage.CollapseType;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.api.RunId;

import java.util.Objects;
import java.util.Set;

/**
 * A {@link Relation} collapsed based on {@link CollapseType}.
 */
public class CollapsedRelation {
  private final NamespacedEntityId data;
  private final ProgramId program;
  private final Set<AccessType> access;
  private final Set<RunId> runs;
  private final Set<NamespacedEntityId> components;

  public CollapsedRelation(DatasetId dataset, ProgramId program, Set<AccessType> access, Set<RunId> runs,
                           Set<NamespacedEntityId> components) {
    this.data = dataset;
    this.program = program;
    this.access = ImmutableSet.copyOf(access);
    this.runs = ImmutableSet.copyOf(runs);
    this.components = ImmutableSet.copyOf(components);
  }

  public CollapsedRelation(StreamId stream, ProgramId program, Set<AccessType> access, Set<RunId> runs,
                           Set<NamespacedEntityId> components) {
    this.data = stream;
    this.program = program;
    this.access = ImmutableSet.copyOf(access);
    this.runs = ImmutableSet.copyOf(runs);
    this.components = ImmutableSet.copyOf(components);
  }

  public NamespacedEntityId getData() {
    return data;
  }

  public ProgramId getProgram() {
    return program;
  }

  public Set<AccessType> getAccess() {
    return access;
  }

  public Set<RunId> getRuns() {
    return runs;
  }

  public Set<NamespacedEntityId> getComponents() {
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
    CollapsedRelation that = (CollapsedRelation) o;
    return Objects.equals(data, that.data) &&
      Objects.equals(program, that.program) &&
      Objects.equals(access, that.access) &&
      Objects.equals(runs, that.runs) &&
      Objects.equals(components, that.components);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, access, runs, components);
  }

  @Override
  public String toString() {
    return "CollapsedRelation{" +
      "data=" + data +
      ", program=" + program +
      ", access=" + access +
      ", runs=" + runs +
      ", components=" + components +
      '}';
  }
}
