/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api;


/**
 * This interface provides specifications for memory in MB and the number of virtual cores.
 */
public interface ResourceSpecification {
  static final int DEFAULT_VIRTUAL_CORES = 1;
  static final int DEFAULT_MEMORY_MB = 512;
  static final ResourceSpecification BASIC =
    Builder.with().setVirtualCores(DEFAULT_VIRTUAL_CORES).setMemoryMB(DEFAULT_MEMORY_MB).build();

  /**
   * Unit for specifying memory size.
   */
  enum SizeUnit {
    MEGA(1),
    GIGA(1024);

    private final int multiplier;

    private SizeUnit(int multiplier) {
      this.multiplier = multiplier;
    }
  }

  /**
   * Returns the number of virtual cores.
   * @return Number of virtual cores.
   */
  int getVirtualCores();

  /**
   * Returns the memory in MB.
   * @return Memory in MB.
   */
  int getMemoryMB();

  /**
   * Class for building {@link ResourceSpecification}.
   */
  static final class Builder {
    private int virtualCores;
    private int memoryMB;

    public static Builder with() {
      return new Builder();
    }

    public Builder setVirtualCores(int cores) {
      virtualCores = cores;
      return this;
    }

    public Builder setMemoryMB(int memory) {
      memoryMB = memory;
      return this;
    }

    public Builder setMemory(int memory, SizeUnit unit) {
      memoryMB = memory * unit.multiplier;
      return this;
    }

    public ResourceSpecification build() {
      return new Resources(memoryMB, virtualCores);
    }

    private Builder() {
      virtualCores = DEFAULT_VIRTUAL_CORES;
      memoryMB = DEFAULT_MEMORY_MB;
    }
  }
}
