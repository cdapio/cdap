/*
 * Copyright © 2014 Cask Data, Inc.
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
 * Carries system resources requirements.
 */
public final class Resources {

  private static final int DEFAULT_VIRTUAL_CORES = 1;
  private static final int DEFAULT_MEMORY_MB = 512;

  private final int virtualCores;
  private final int memoryMB;

  /**
   * Constructs a Resources instance that represents 1 virtual core and 512MB of memory.
   */
  public Resources() {
    this(DEFAULT_MEMORY_MB, DEFAULT_VIRTUAL_CORES);
  }

  /**
   * Constructs a Resources instance that represents 1 virtual core and custom memory size.
   *
   * @param memoryMB memory requirement in MB.
   */
  public Resources(int memoryMB) {
    this(memoryMB, DEFAULT_VIRTUAL_CORES);
  }

  /**
   * Constructs a Resources instance.
   *
   * @param memoryMB memory requirement in MB.
   * @param cores number of virtual cores.
   */
  public Resources(int memoryMB, int cores) {
    this.memoryMB = memoryMB;
    this.virtualCores = cores;
  }

  /**
   * Returns the number of virtual cores.
   */
  public int getVirtualCores() {
    return virtualCores;
  }

  /**
   * Returns the memory size in MB.
   */
  public int getMemoryMB() {
    return memoryMB;
  }
}
