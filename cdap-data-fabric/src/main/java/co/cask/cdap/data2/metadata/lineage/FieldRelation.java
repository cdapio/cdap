/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.proto.id.FieldEntityId;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;

import java.util.Objects;

/**
 * Represents a Field access by a Program.
 */
public class FieldRelation {
  private final FieldEntityId data;
  private final ProgramId program;
  private final AccessType access;
  private final RunId run;

  public FieldRelation(FieldEntityId data, ProgramId program, AccessType access, RunId run) {
    this.data = data;
    this.program = program;
    this.access = access;
    this.run = run;
  }

  public FieldEntityId getData() {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FieldRelation that = (FieldRelation) o;
    return Objects.equals(this.data, that.data) &&
      Objects.equals(this.program, that.program) &&
      Objects.equals(this.access, that.access) &&
      Objects.equals(this.run, that.run);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, program, access, run);
  }

  @Override
  public String toString() {
    return "Relation{" +
      "data=" + data +
      ", program=" + program +
      ", access=" + access +
      ", runs=" + run +
      '}';
  }
}
