/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security;

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
