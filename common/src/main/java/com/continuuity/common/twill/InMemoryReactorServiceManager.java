package com.continuuity.common.twill;

/**
 * InMemory Reactor Service Management class.
 */
public class InMemoryReactorServiceManager implements ReactorServiceManager {

  @Override
  public int getRequestedInstances() {
    return 1;
  }

  @Override
  public int getProvisionedInstances() {
    return 1;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    return false;
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public int getMaxInstances() {
    return 1;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isServiceAvailable() {
    return true;
  }
}
