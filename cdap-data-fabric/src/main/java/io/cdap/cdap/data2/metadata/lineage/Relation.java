/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.lineage;

import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a Dataset access by a Program.
 */
public class Relation {
  private final NamespacedEntityId data;
  private final ProgramId program;
  private final AccessType access;
  private final RunId run;
  private final Set<NamespacedEntityId> components;
  // Don't serialize this id
  private final transient ProgramRunId runId;

  public Relation(DatasetId data, ProgramId program, AccessType access, RunId run) {
    this.data = data;
    this.program = program;
    this.access = access;
    this.run = run;
    this.components = Collections.emptySet();
    this.runId = program.run(run);
  }

  public NamespacedEntityId getData() {
    return data;
  }

  public ProgramId getProgram() {
    return program;
  }

  public AccessType getAccess() {
    return access;
  }

  public RunId getRun() {
    return run;
  }

  public Set<NamespacedEntityId> getComponents() {
    return components;
  }

  public ProgramRunId getProgramRunId() {
    return runId;
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
      ", run=" + run +
      ", components=" + components +
      '}';
  }
}
