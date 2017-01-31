/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.utils;

import org.junit.Assert;
import org.junit.Test;

public class FileUtilsTest {

  @Test
  public void testPermissionsToUmask() {
    Assert.assertEquals(0000, FileUtils.permissionsToUmask(0777));
    Assert.assertEquals(0022, FileUtils.permissionsToUmask(0755));
    Assert.assertEquals(0002, FileUtils.permissionsToUmask(0775));
    Assert.assertEquals(0137, FileUtils.permissionsToUmask(0640));
    Assert.assertEquals(0077, FileUtils.permissionsToUmask(0700));
    Assert.assertEquals(0777, FileUtils.permissionsToUmask(0000));

    Assert.assertEquals("000", FileUtils.permissionsToUmask("777"));
    Assert.assertEquals("022", FileUtils.permissionsToUmask("755"));
    Assert.assertEquals("002", FileUtils.permissionsToUmask("775"));
    Assert.assertEquals("137", FileUtils.permissionsToUmask("640"));
    Assert.assertEquals("077", FileUtils.permissionsToUmask("700"));
    Assert.assertEquals("777", FileUtils.permissionsToUmask("000"));

    Assert.assertEquals("000", FileUtils.permissionsToUmask("rwxrwxrwx"));
    Assert.assertEquals("022", FileUtils.permissionsToUmask("rwxr-xr-x"));
    Assert.assertEquals("002", FileUtils.permissionsToUmask("rwxrwxr-x"));
    Assert.assertEquals("137", FileUtils.permissionsToUmask("rw-r-----"));
    Assert.assertEquals("077", FileUtils.permissionsToUmask("rwx------"));
    Assert.assertEquals("777", FileUtils.permissionsToUmask("---------"));
  }

  @Test
  public void testParsePermissions() {
    Assert.assertEquals(0000, FileUtils.parsePermissions("---------"));
    Assert.assertEquals(0750, FileUtils.parsePermissions("rwxr-x---"));
    Assert.assertEquals(0755, FileUtils.parsePermissions("rwxr-xr-x"));
    Assert.assertEquals(0700, FileUtils.parsePermissions("rwx------"));
    Assert.assertEquals(0644, FileUtils.parsePermissions("rw-r--r--"));
    Assert.assertEquals(0640, FileUtils.parsePermissions("rw-r-----"));
    Assert.assertEquals(0330, FileUtils.parsePermissions("-wx-wx---"));

    Assert.assertEquals(0000, FileUtils.parsePermissions("000"));
    Assert.assertEquals(0750, FileUtils.parsePermissions("750"));
    Assert.assertEquals(0755, FileUtils.parsePermissions("755"));
    Assert.assertEquals(0700, FileUtils.parsePermissions("700"));
    Assert.assertEquals(0644, FileUtils.parsePermissions("644"));
    Assert.assertEquals(0640, FileUtils.parsePermissions("640"));
    Assert.assertEquals(0330, FileUtils.parsePermissions("330"));

    String[] illegalPermissions = new String[] {
      "0000", "rwx", "00",
      "drwxrwxrwx", "wrx------", "rwxrwxrws", "write_all",
      "twx------", "rtx------", "rwt------"
    };
    for (String perm : illegalPermissions) {
      try {
        FileUtils.parsePermissions(perm);
        Assert.fail("Expected IllegalArgumentException for illegal permission string " + perm);
      } catch (IllegalArgumentException e) {
        // expected
      }
    }

    String perm = "080";
    try {
      FileUtils.parsePermissions(perm);
      Assert.fail("Expected NumberFormatException for illegal permission string" + perm);
    } catch (NumberFormatException e) {
      // expected
    }

  }
}
