/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.master.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link UpgradeUserIDProvider}.
 */
public class UpgradeUserIDProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGetUserIDReturnsShortUserName() throws IOException {
    Assert.assertEquals(
        UserGroupInformation.getCurrentUser().getShortUserName(),
        UpgradeUserIDProvider.getUserID());
  }

  @Test
  public void testGetUserIDWrapsIOExceptionWhenCurrentUserLoginFails() throws IOException {
    UserGroupInformation previousLoginUser = UserGroupInformation.getLoginUser();
    String previousTokenFiles = System.getProperty("hadoop.token.files");

    File corruptTokenFile = TEMP_FOLDER.newFile("corrupt-hadoop.tokens");
    Files.write(corruptTokenFile.toPath(), "not-a-valid-token-storage-header".getBytes(StandardCharsets.UTF_8));

    try {
      System.setProperty("hadoop.token.files", corruptTokenFile.getAbsolutePath());
      UserGroupInformation.setLoginUser(null);
      try {
        UpgradeUserIDProvider.getUserID();
        Assert.fail("Expected RuntimeException wrapping IOException from failed UGI login");
      } catch (RuntimeException actual) {
        Assert.assertTrue(
            "Expected IOException cause, got: " + actual.getCause(),
            actual.getCause() instanceof IOException);
      }
    } finally {
      if (previousTokenFiles == null) {
        System.clearProperty("hadoop.token.files");
      } else {
        System.setProperty("hadoop.token.files", previousTokenFiles);
      }
      UserGroupInformation.setLoginUser(previousLoginUser);
    }
  }
}
