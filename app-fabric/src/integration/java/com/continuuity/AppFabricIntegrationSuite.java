/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({})
public class AppFabricIntegrationSuite {

  /**
   * This allows us to not duplicate resource management code in each
   * class, you can resue your code, but instead of putting this common
   * in superclass for your tests, you can abstract external resource
   * management with {@link ExternalResource}.
   */
  @ClassRule
  public static ExternalResource testRule = new ExternalResource() {
    /**
     * Override to set up your specific external resource.
     *
     * @throws if setup fails (which will disable {@code after}
     */
    @Override
    protected void before() throws Throwable {
      super.before();
    }

    /**
     * Override to tear down your specific external resource.
     */
    @Override
    protected void after() {
      super.after();
    }
  };

}
