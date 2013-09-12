package com.continuuity.internal;

import com.continuuity.api.ResourceSpecification;

/**
 * Straightforward implementation of {@link ResourceSpecification}.
 */
public final class DefaultResourceSpecification implements ResourceSpecification {
  private int virtualCores;
  private int memoryMB;

  public static ResourceSpecification create() {
    return new DefaultResourceSpecification(ResourceSpecification.DEFAULT_VIRTUAL_CORES,
                                            ResourceSpecification.DEFAULT_MEMORY_MB);
  }

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

  @Override
  public void setVirtualCores(int cores) {
    this.virtualCores = cores;
  }

  @Override
  public void setMemoryMB(int memory) {
    this.memoryMB = memory;
  }

  @Override
  public void setMemory(int memory, SizeUnit unit) {
    this.memoryMB = memory * unit.getMultiplier();
  }
}
