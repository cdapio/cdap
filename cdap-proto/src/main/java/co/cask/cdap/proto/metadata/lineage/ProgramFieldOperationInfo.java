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

import co.cask.cdap.api.annotation.Beta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In field lineage details, represents a set of programs that performed the same operations.
 */
@Beta
public class ProgramFieldOperationInfo {
  private final List<ProgramInfo> programs;
  private final List<FieldOperationInfo> operations;

  public ProgramFieldOperationInfo(List<ProgramInfo> programs, List<FieldOperationInfo> operations) {
    this.programs = Collections.unmodifiableList(new ArrayList<>(programs));
    this.operations = Collections.unmodifiableList(new ArrayList<>(operations));
  }

  public List<ProgramInfo> getPrograms() {
    return programs;
  }

  public List<FieldOperationInfo> getOperations() {
    return operations;
  }
}
