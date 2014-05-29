package com.continuuity.common.twill;

/**
 *
 */
public class InMemoryReactorServiceManagement implements ReactorServiceManagement {

  @Override
  public int getInstanceCount() {
    return 1;
  }

  @Override
  public boolean setInstanceCount(int instanceCount) {
    return false;
  }
}
