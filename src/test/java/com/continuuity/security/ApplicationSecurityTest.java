package com.continuuity.security;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests Capabilities of Security Manager to restrict access.
 */
public class ApplicationSecurityTest {

  /**
   * Tests that the admin class has ability to change security manager.
   */
  @Test
  public void testAbilityToSetSecurityManagerMultipleTimes() throws Exception {
    // Apply security and make sure we block exitJVM. This would throw an
    // SecurityException.
    ApplicationSecurity.builder()
      .adminClass(this.getClass())
      .add(new RuntimePermission("exitJVM"))
      .apply();

    try {
      System.exit(0);
      Assert.assertFalse("Security is not set correctly. System.exit should have thrown security exception", true);
    } catch (Exception e) {
      Assert.assertTrue("Security manager was set", true);
    }

    // Changes the security setting.
    ApplicationSecurity.builder()
      .adminClass(this.getClass())
      .add(new RuntimePermission("exitJVM.2"))
      .apply();

    try {
      System.exit(2);
      Assert.assertFalse("Security is not set correctly. System.exit should have thrown security exception", true);
    } catch (Exception e) {
      Assert.assertTrue("Security manager was set again.", true);
    }
  }
}
