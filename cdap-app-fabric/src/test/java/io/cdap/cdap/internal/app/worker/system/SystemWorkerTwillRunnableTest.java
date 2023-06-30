/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SystemWorkerTwillRunnableTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    Injector injector = SystemWorkerTwillRunnable
      .createInjector(CConfiguration.create(), new Configuration(), SConfiguration.create());
    injector.getInstance(ArtifactManagerFactory.class).create(NamespaceId.SYSTEM, RetryStrategies.noRetry());
  }

  @Test
  public void testDoInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File cConfFileTemp = temporaryFolder.newFile("cConf.xml");
    File hConfFileTemp = temporaryFolder.newFile("hConf.xml");
    File sConfFileTemp = temporaryFolder.newFile("sConf.xml");
    try (FileOutputStream fileOutputStream = new FileOutputStream(cConfFileTemp.getAbsolutePath())) {
      cConf.writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(hConfFileTemp.getAbsolutePath())) {
      new io.cdap.cdap.common.conf.Configuration().writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(sConfFileTemp.getAbsolutePath())) {
      new io.cdap.cdap.common.conf.Configuration().writeXml(fileOutputStream);
    }

    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    SystemWorkerTwillRunnable systemWorkerTwillRunnable = new SystemWorkerTwillRunnable(
      cConfFileTemp.getAbsolutePath(), hConfFileTemp.getAbsolutePath(), sConfFileTemp.getAbsolutePath());
    systemWorkerTwillRunnable.doInitialize();
  }
}
