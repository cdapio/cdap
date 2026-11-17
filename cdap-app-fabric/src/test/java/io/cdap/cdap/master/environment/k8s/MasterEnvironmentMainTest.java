/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.security.auth.TokenManager;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link MasterEnvironmentMain}.
 */
public class MasterEnvironmentMainTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testDoMainMissingRunnableClassThrows() throws Exception {
    try {
      MasterEnvironmentMain.doMain(new String[0]);
      Assert.fail("Expected IllegalArgumentException when runnable class is missing");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Missing runnable class name"));
    }
  }

  @Test
  public void testGetInternalAuthenticatorStartsAndStopsTokenManager() throws Exception {
    Method authMethod =
        MasterEnvironmentMain.class.getDeclaredMethod(
            "getInternalAuthenticator", CConfiguration.class);
    authMethod.setAccessible(true);
    Field tokenManagerField = MasterEnvironmentMain.class.getDeclaredField("tokenManager");
    tokenManagerField.setAccessible(true);

    // Case 1: secret file absent -> worker authentication context, tokenManager not created.
    CConfiguration workerConf = CConfiguration.create();
    File missingSecret = new File(TEMP_FOLDER.newFolder(), "nonexistent-secret");
    workerConf.set(
        Constants.Twill.Security.MASTER_SECRET_DISK_PATH, missingSecret.getAbsolutePath());
    InternalAuthenticator workerAuth =
        (InternalAuthenticator) authMethod.invoke(null, workerConf);
    Assert.assertNotNull(workerAuth);
    Assert.assertNull(tokenManagerField.get(null));

    // Case 2: secret file present + internal auth enabled -> starts TokenManager via startAndWait.
    File secretFile = TEMP_FOLDER.newFile("cdap-secret");
    File keyDir = TEMP_FOLDER.newFolder("keydir");
    CConfiguration masterConf = CConfiguration.create();
    masterConf.set(Constants.Twill.Security.MASTER_SECRET_DISK_PATH, secretFile.getAbsolutePath());
    masterConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    masterConf.set(
        Constants.Security.CFG_FILE_BASED_KEYFILE_PATH,
        new File(keyDir, "keyfile").getAbsolutePath());

    try {
      InternalAuthenticator masterAuth =
          (InternalAuthenticator) authMethod.invoke(null, masterConf);
      Assert.assertNotNull(masterAuth);
      TokenManager manager = (TokenManager) tokenManagerField.get(null);
      Assert.assertNotNull(manager);
      Assert.assertEquals(Service.State.RUNNING, manager.state());

      manager.stopAsync().awaitTerminated();
      Assert.assertEquals(Service.State.TERMINATED, manager.state());
    } finally {
      TokenManager manager = (TokenManager) tokenManagerField.get(null);
      if (manager != null && manager.isRunning()) {
        manager.stopAsync().awaitTerminated();
      }
      tokenManagerField.set(null, null);
    }
  }
}
