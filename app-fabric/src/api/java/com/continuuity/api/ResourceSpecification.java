package com.continuuity.api;


import com.continuuity.internal.DefaultResourceSpecification;

/**
 * This interface provides specifications for resource requirements, which currently include
 * number of cores and amount of memory in megabytes.
 */
public interface ResourceSpecification {

  final ResourceSpecification BASIC = Builder.with().setCores(1).setMemory(512, SizeUnit.MEGA).build();

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
  int getCores();

  /**
   * Returns the memory size in MB.
   * @return Memory size
   */
  int getMemorySize();

  /**
   * Builder for creating {@link ResourceSpecification}.
   */
  static final class Builder {

    private int cores;
    private int memory;

    public static Builder with() {
      return new Builder();
    }

    public Builder setCores(int cores) {
      Builder.this.cores = cores;
      return Builder.this;
    }

    public Builder setMemory(int size, SizeUnit unit) {
      Builder.this.memory = size * unit.multiplier;
      return Builder.this;
    }

    public ResourceSpecification build() {
      return new DefaultResourceSpecification(cores, memory);
    }

    private Builder() {
      this.cores = 1;
      this.memory = 512;
    }
  }
}
