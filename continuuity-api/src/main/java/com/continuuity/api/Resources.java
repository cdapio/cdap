/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api;

/**
 * Implementation of {@link ResourceSpecification}.
 */
public final class Resources implements ResourceSpecification {
  private final int virtualCores;
  private final int memoryMB;

  public Resources() {
    this(ResourceSpecification.DEFAULT_MEMORY_MB, ResourceSpecification.DEFAULT_VIRTUAL_CORES);
  }

  public Resources(int memoryMB) {
    this(memoryMB, ResourceSpecification.DEFAULT_VIRTUAL_CORES);
  }

  public Resources(int memoryMB, int cores) {
    this.memoryMB = memoryMB;
    this.virtualCores = cores;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public int getMemoryMB() {
    return memoryMB;
  }
}
