package com.continuuity.api;


import com.continuuity.internal.DefaultResourceSpecification;

/**
 * This interface provides specifications for resource requirements, which currently include
 * number of cores and amount of memory in megabytes.
 */
public interface ResourceSpecification {

  final ResourceSpecification BASIC = Builder.with().setVirtualCores(1).setMemory(512, SizeUnit.MEGA).build();
  static final int DEFAULT_VIRTUAL_CORES = 1;
  static final int DEFAULT_MEMORY_MB = 512;

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
   * Returns the number of CPU cores.
   * @return Number of CPU cores.
   */
  int getVirtualCores();

  /**
   * Returns the memory in MB.
   * @return Memory in MB
   */
  int getMemoryMB();

  /**
   * Builder for creating {@link ResourceSpecification}.
   */
  static final class Builder {

    private int cores;
    private int memorySize;

    public static Builder with() {
      return new Builder();
    }

    public Builder setVirtualCores(int cores) {
      Builder.this.cores = cores;
      return Builder.this;
    }

    public Builder setMemoryMB(int size) {
      return Builder.this.setMemory(size, SizeUnit.MEGA);
    }

    public Builder setMemory(int size, SizeUnit unit) {
      Builder.this.memorySize = size * unit.multiplier;
      return Builder.this;
    }

    public ResourceSpecification build() {
      return new DefaultResourceSpecification(cores, memorySize);
    }

    private Builder() {
      this.cores = DEFAULT_VIRTUAL_CORES;
      this.memorySize = DEFAULT_MEMORY_MB;
    }
  }
}
