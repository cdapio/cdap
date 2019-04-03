/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.metadata.lineage;

import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a set of program runs that performed the same set of field operations.
 */
public class ProgramRunOperations {
  private final Set<ProgramRunId> programRunIds;
  private final Set<Operation> operations;

  public ProgramRunOperations(Set<ProgramRunId> programRunIds, Set<Operation> operations) {
    this.programRunIds = programRunIds;
    this.operations = operations;
  }

  public Set<ProgramRunId> getProgramRunIds() {
    return programRunIds;
  }

  public Set<Operation> getOperations() {
    return operations;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramRunOperations that = (ProgramRunOperations) o;
    return Objects.equals(programRunIds, that.programRunIds) &&
            Objects.equals(operations, that.operations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(programRunIds, operations);
  }
}
