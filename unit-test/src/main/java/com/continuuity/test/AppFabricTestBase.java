package com.continuuity.test;

/**
 * Base class to inherit from, provides testing functionality for {@link com.continuuity.api.Application}.
 * @deprecated As of 1.6.0, replaced by {@link com.continuuity.test.ReactorTestBase}.
 */
@Deprecated
public class AppFabricTestBase extends ReactorTestBase {
  /**
   * @deprecated As of 1.6.0, replaced by {@link #clear()}.
   */
  @Deprecated
  protected void clearAppFabric() {
    clear();
  }
}

