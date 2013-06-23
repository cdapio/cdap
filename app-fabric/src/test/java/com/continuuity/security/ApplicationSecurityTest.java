package com.continuuity.security;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests Capabilities of Security Manager to restrict access.
 */
public class ApplicationSecurityTest {

  private static final class SecurityAdmin {
    private void set(RuntimePermission permission) {
      ApplicationSecurity.builder()
        .adminClass(this.getClass())
        .add(permission)
        .apply();
    }

    private void reset() {
      System.setSecurityManager(null);
    }
  }

  /**
   * Tests that the admin class has ability to change security manager.
   */
  @Test
  @Ignore
  public void testAbilityToSetSecurityManagerMultipleTimes() throws Exception {
    SecurityAdmin admin = new SecurityAdmin();
    try {
      // Apply security and make sure we block exitJVM. This would throw an
      // SecurityException.
      admin.set(new RuntimePermission("exitJVM"));

      try {
        System.exit(0);
        Assert.assertFalse("Security is not set correctly. System.exit should have thrown security exception", true);
      } catch (Exception e) {
        Assert.assertTrue("Security manager was set", true);
      }

      // Changes the security setting.
      admin.set(new RuntimePermission("exitJVM.2"));

      try {
        System.exit(2);
        Assert.assertFalse("Security is not set correctly. System.exit should have thrown security exception", true);
      } catch (Exception e) {
        Assert.assertTrue("Security manager was set again.", true);
      }
    } finally {
      admin.reset();
    }
  }
}
