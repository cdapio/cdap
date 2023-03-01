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

package io.cdap.cdap.internal.app.worker.sidecar;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Configuration;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.master.environment.MasterEnvironments;
import java.io.File;
import java.io.FileOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the Artifact Localizer Twill runnable.
 */
public class ArtifactLocalizerTwillRunnableTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDoInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File temporaryCConfFile = temporaryFolder.newFile("cConf.xml");
    File temporaryHConfFile = temporaryFolder.newFile("hConf.xml");
    try (FileOutputStream fileOutputStream = new FileOutputStream(temporaryCConfFile.getAbsolutePath())) {
      cConf.writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(temporaryHConfFile.getAbsolutePath())) {
      new Configuration().writeXml(fileOutputStream);
    }
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    ArtifactLocalizerTwillRunnable artifactLocalizerTwillRunnable = new ArtifactLocalizerTwillRunnable(
      temporaryCConfFile.getAbsolutePath(), temporaryHConfFile.getAbsolutePath());
    artifactLocalizerTwillRunnable.doInitialize();
  }
}
