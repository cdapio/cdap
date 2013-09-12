package com.continuuity.internal;

import com.continuuity.api.ResourceSpecification;

/**
 * Straightforward implementation of {@link ResourceSpecification}.
 */
public final class DefaultResourceSpecification implements ResourceSpecification {
  private final int virtualCores;
  private final int memoryMB;

  public DefaultResourceSpecification(int memoryMB) {
    this(memoryMB, 1);
  }

  public DefaultResourceSpecification(int memoryMB, int cores) {
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
