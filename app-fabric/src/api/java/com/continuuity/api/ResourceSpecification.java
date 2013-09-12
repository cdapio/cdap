package com.continuuity.api;


import com.continuuity.internal.DefaultResourceSpecification;

/**
 * This interface provides specifications for resource requirements, which currently include
 * number of cores and amount of memory in megabytes.
 */
public interface ResourceSpecification {
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

    public int getMultiplier() {
      return this.multiplier;
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
   * Set the number of virtual cores.
   * @param cores Number of virtual cores.
   */
  void setVirtualCores(int cores);

  /**
   * Set the amount of memory in MB.
   * @param memory Amount of memory in MB.
   */
  void setMemoryMB(int memory);

  /**
   *
   * @param memory
   * @param unit
   */
  void setMemory(int memory, SizeUnit unit);
}
