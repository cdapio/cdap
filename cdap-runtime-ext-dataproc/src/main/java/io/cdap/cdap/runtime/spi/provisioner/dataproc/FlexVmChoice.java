/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * One candidate machine type in a Dataproc instance flexibility policy, holding the resolved
 * machine type, its priority rank, and an optional boot disk type. Choices with the same rank and
 * disk type are grouped into a single instance selection.
 */
final class FlexVmChoice {

  private final int rank;
  private final String machineType;
  @Nullable
  private final String diskType;

  FlexVmChoice(int rank, String machineType, @Nullable String diskType) {
    this.rank = rank;
    this.machineType = machineType;
    this.diskType = diskType;
  }

  int getRank() {
    return rank;
  }

  String getMachineType() {
    return machineType;
  }

  @Nullable
  String getDiskType() {
    return diskType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlexVmChoice that = (FlexVmChoice) o;
    return rank == that.rank
        && machineType.equals(that.machineType)
        && Objects.equals(diskType, that.diskType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rank, machineType, diskType);
  }

  @Override
  public String toString() {
    return "FlexVmChoice{rank=" + rank + ", machineType=" + machineType
        + ", diskType=" + diskType + '}';
  }
}
