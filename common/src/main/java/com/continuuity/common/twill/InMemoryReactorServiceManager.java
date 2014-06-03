package com.continuuity.common.twill;

/**
 * InMemory Reactor Service Management class.
 */
public class InMemoryReactorServiceManager implements ReactorServiceManager {

  @Override
  public int getInstances() {
    return 1;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return false;
  }
}
