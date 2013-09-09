package com.continuuity.internal;

import com.continuuity.api.ResourceSpecification;

/**
 * Straightforward implementation of {@link ResourceSpecification}.
 */
public final class DefaultResourceSpecification implements ResourceSpecification {

  private final int cores;
  private final int memorySize;

  public DefaultResourceSpecification(int cores, int memorySize) {
    this.cores = cores;
    this.memorySize = memorySize;
  }
  @Override
  public int getCores() {
    return cores;
  }

  @Override
  public int getMemorySize() {
    return memorySize;
  }
}
