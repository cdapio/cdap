/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Namespace and resource quota associated with a tether.
 */
public class NamespaceAllocation {
  private final String namespace;
  private final String cpuLimit;
  private final String memoryLimit;

  public NamespaceAllocation(String namespace, @Nullable String cpuLimit, @Nullable String memoryLimit) {
    this.namespace = namespace;
    this.cpuLimit = cpuLimit;
    this.memoryLimit = memoryLimit;
  }

  public String getNamespace() {
    return namespace;
  }

  @Nullable
  public String getCpuLimit() {
    return cpuLimit;
  }

  @Nullable
  public String getMemoryLimit() {
    return memoryLimit;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    NamespaceAllocation that = (NamespaceAllocation) other;
    return Objects.equals(this.namespace, that.namespace) &&
      Objects.equals(this.cpuLimit, that.cpuLimit) &&
      Objects.equals(this.memoryLimit, that.memoryLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, cpuLimit, memoryLimit);
  }
}
