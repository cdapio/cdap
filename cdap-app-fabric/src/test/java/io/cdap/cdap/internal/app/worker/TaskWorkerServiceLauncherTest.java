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

package io.cdap.cdap.internal.app.worker;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.master.spi.twill.SecretDisk;
import io.cdap.cdap.master.spi.twill.SecureTwillPreparer;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests the secret disks that {@link TaskWorkerServiceLauncher} mounts into task workers.
 */
public class TaskWorkerServiceLauncherTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String KEY_FILE = "/etc/cdap/security/tokenManagerKey.json";

  @Test
  public void testMountsOnlyKeyFileWhenInternalAuthIsEnabled() throws IOException {
    List<SecretDisk> disks = getTaskWorkerSecretDisks(createConf(false, true));

    Assert.assertEquals(1, disks.size());
    Assert.assertEquals("cdap-security", disks.get(0).getName());
    Assert.assertEquals(KEY_FILE, disks.get(0).getPath());
    Assert.assertEquals("tokenManagerKey.json", disks.get(0).getSubPath());
  }

  @Test
  public void testMountsNothingWithoutInternalAuth() throws IOException {
    Assert.assertEquals(Collections.emptyList(),
        getTaskWorkerSecretDisks(createConf(false, false)));
  }

  @Test
  public void testMountsNothingWithZookeeperQuorum() throws IOException {
    CConfiguration cConf = createConf(false, true);
    cConf.set(Constants.Zookeeper.QUORUM, "localhost:2181");
    cConf.unset(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH);

    Assert.assertEquals(Collections.emptyList(), getTaskWorkerSecretDisks(cConf));
  }

  @Test
  public void testSkipsLaunchWhenKeyFileIsMissing() throws IOException {
    CConfiguration cConf = createConf(false, true);
    cConf.unset(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH);
    SecureTwillPreparer preparer = mockPreparer();

    new TaskWorkerServiceLauncher(cConf, new Configuration(), mockTwillRunner(preparer)).run();

    Mockito.verify(preparer, Mockito.never()).start(Mockito.anyLong(), Mockito.any(TimeUnit.class));
  }

  @Test
  public void testKeepsWholeSecretMount() throws IOException {
    List<SecretDisk> disks = getTaskWorkerSecretDisks(createConf(true, true));

    Assert.assertEquals(1, disks.size());
    Assert.assertEquals("/etc/cdap/security", disks.get(0).getPath());
    Assert.assertNull(disks.get(0).getSubPath());
  }

  private CConfiguration createConf(boolean mountWorkerSecret,
      boolean internalAuthEnabled) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Twill.Security.WORKER_MOUNT_SECRET, mountWorkerSecret);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, internalAuthEnabled);
    cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, KEY_FILE);
    return cConf;
  }

  private SecureTwillPreparer mockPreparer() {
    SecureTwillPreparer preparer = Mockito.mock(SecureTwillPreparer.class, Mockito.RETURNS_SELF);
    TwillController controller = Mockito.mock(TwillController.class);
    Mockito.when(preparer.start(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
        .thenReturn(controller);
    return preparer;
  }

  private TwillRunner mockTwillRunner(SecureTwillPreparer preparer) {
    TwillRunner twillRunner = Mockito.mock(TwillRunner.class);
    Mockito.when(twillRunner.lookup(TaskWorkerTwillApplication.NAME))
        .thenReturn(Collections.emptyList());
    Mockito.when(twillRunner.prepare(Mockito.any(TwillApplication.class))).thenReturn(preparer);
    return twillRunner;
  }

  /**
   * Launches the task worker pool with a mock preparer and returns the secret disks of the task
   * worker container.
   */
  private List<SecretDisk> getTaskWorkerSecretDisks(CConfiguration cConf) {
    SecureTwillPreparer preparer = mockPreparer();

    new TaskWorkerServiceLauncher(cConf, new Configuration(), mockTwillRunner(preparer)).run();

    Mockito.verify(preparer).start(Mockito.anyLong(), Mockito.any(TimeUnit.class));
    ArgumentCaptor<SecretDisk> disks = ArgumentCaptor.forClass(SecretDisk.class);
    Mockito.verify(preparer, Mockito.atMost(1))
        .withSecretDisk(Mockito.eq(TaskWorkerTwillRunnable.class.getSimpleName()), disks.capture());
    return disks.getAllValues();
  }
}
