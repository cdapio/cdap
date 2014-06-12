package com.continuuity.api;


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
