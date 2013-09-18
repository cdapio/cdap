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
