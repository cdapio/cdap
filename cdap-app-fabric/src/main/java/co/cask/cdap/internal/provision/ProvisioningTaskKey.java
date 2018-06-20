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

package co.cask.cdap.internal.provision;

import co.cask.cdap.proto.id.ProgramRunId;

import java.util.Objects;

/**
 * Key to identify a provisioning task.
 */
public class ProvisioningTaskKey {
  private final ProgramRunId programRunId;
  private final ProvisioningOp.Type type;

  public ProvisioningTaskKey(ProgramRunId programRunId, ProvisioningOp.Type type) {
    this.programRunId = programRunId;
    this.type = type;
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public ProvisioningOp.Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ProvisioningTaskKey that = (ProvisioningTaskKey) o;

    return Objects.equals(programRunId, that.programRunId) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(programRunId, type);
  }
}
