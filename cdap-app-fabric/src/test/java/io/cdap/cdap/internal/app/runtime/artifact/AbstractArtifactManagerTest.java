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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests {@link AbstractArtifactManager} classloader behavior on Java 11.
 */
public class AbstractArtifactManagerTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  /**
   * Standalone class packaged into the test artifact jar with no parent CDAP API dependencies.
   */
  public static class SampleArtifactClass {
  }

  @Test
  public void testNullParentLoadsPlatformModules() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    Location jarLocation = AppJarHelper.createDeploymentJar(
        new LocalLocationFactory(TMP_FOLDER.newFolder()), SampleArtifactClass.class);
    ArtifactInfo artifactInfo = new ArtifactInfo(
        "sample-artifact", "1.0.0", ArtifactScope.USER, ArtifactClasses.builder().build(),
        Collections.emptyMap());

    AbstractArtifactManager manager = new AbstractArtifactManager(cConf) {
      @Override
      public List<ArtifactInfo> listArtifacts() {
        return Collections.singletonList(artifactInfo);
      }

      @Override
      public List<ArtifactInfo> listArtifacts(String namespace) {
        return listArtifacts();
      }

      @Override
      protected Location getArtifactLocation(ArtifactInfo info, @Nullable String namespace)
          throws IOException {
        return jarLocation;
      }
    };

    try (CloseableClassLoader classLoader = manager.createClassLoader(artifactInfo, null)) {
      Assert.assertNotNull(classLoader.loadClass(SampleArtifactClass.class.getName()));
      Assert.assertSame(Class.forName("java.sql.Driver"), classLoader.loadClass("java.sql.Driver"));
      Assert.assertSame(Class.forName("org.ietf.jgss.GSSManager"),
          classLoader.loadClass("org.ietf.jgss.GSSManager"));
    }
  }
}
