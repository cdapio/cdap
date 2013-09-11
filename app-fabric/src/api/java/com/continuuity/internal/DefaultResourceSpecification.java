package com.continuuity.internal;

import com.continuuity.api.ResourceSpecification;

/**
 * Straightforward implementation of {@link ResourceSpecification}.
 */
public final class DefaultResourceSpecification implements ResourceSpecification {

  private final int virtualCores;
  private final int memoryMB;

  public DefaultResourceSpecification(int cores, int memoryMB) {
    this.virtualCores = cores;
    this.memoryMB = memoryMB;
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
