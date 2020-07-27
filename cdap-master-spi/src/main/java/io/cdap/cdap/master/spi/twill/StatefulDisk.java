/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;

import java.util.Objects;

/**
 * Represents information about a stateful disk used by a twill runnable.
 */
public class StatefulDisk {

  private final String name;
  private final int diskSizeGB;
  private final String mountPath;

  public StatefulDisk(String name, int diskSizeGB, String mountPath) {
    this.name = name;
    this.diskSizeGB = diskSizeGB;
    this.mountPath = mountPath;
  }

  public String getName() {
    return name;
  }

  public int getDiskSizeGB() {
    return diskSizeGB;
  }

  public String getMountPath() {
    return mountPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StatefulDisk that = (StatefulDisk) o;
    return diskSizeGB == that.diskSizeGB &&
      Objects.equals(name, that.name) &&
      Objects.equals(mountPath, that.mountPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, diskSizeGB, mountPath);
  }
}
