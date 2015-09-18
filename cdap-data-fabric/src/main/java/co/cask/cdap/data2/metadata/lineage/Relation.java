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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a Dataset access by a Program.
 */
public class Relation {
  private final Id.NamespacedId data;
  private final Id.Program program;
  private final AccessType access;
  private final RunId run;
  private final Set<Id.NamespacedId> components;

  public Relation(Id.DatasetInstance data, Id.Program program, AccessType access, RunId run) {
    this(data, program, access, run, Collections.<Id.NamespacedId>emptySet());
  }

  public Relation(Id.DatasetInstance data, Id.Program program, AccessType access, RunId run,
                  Set<? extends Id.NamespacedId> components) {
    this.data = data;
    this.program = program;
    this.access = access;
    this.run = run;
    this.components = ImmutableSet.copyOf(components);
  }

  public Relation(Id.Stream stream, Id.Program program, AccessType access, RunId run) {
    this(stream, program, access, run, Collections.<Id.NamespacedId>emptySet());
  }

  public Relation(Id.Stream stream, Id.Program program, AccessType access, RunId run,
                  Set<? extends Id.NamespacedId> components) {
    this.data = stream;
    this.program = program;
    this.access = access;
    this.run = run;
    this.components = ImmutableSet.copyOf(components);
  }

  public Id.NamespacedId getData() {
    return data;
  }

  public Id.Program getProgram() {
    return program;
  }

  public AccessType getAccess() {
    return access;
  }

  public RunId getRun() {
    return run;
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
      Objects.equals(run, relation.run) &&
      Objects.equals(components, relation.components);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, access, run, components);
  }

  @Override
  public String toString() {
    return "Relation{" +
      "data=" + data +
      ", program=" + program +
      ", access=" + access +
      ", runs=" + run +
      ", components=" + components +
      '}';
  }
}
