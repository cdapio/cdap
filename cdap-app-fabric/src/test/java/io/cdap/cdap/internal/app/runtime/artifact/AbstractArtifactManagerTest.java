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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.EmptyClass;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link AbstractArtifactManager}.
 */
public class AbstractArtifactManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static Location createMinimalJar(LocalLocationFactory locationFactory,
      File jarFile, Class<?>... classes) throws IOException {
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
      byte[] buffer = new byte[4096];
      for (Class<?> cls : classes) {
        String entryName = cls.getName().replace('.', '/') + ".class";
        try (InputStream is = cls.getClassLoader().getResourceAsStream(entryName)) {
          Assert.assertNotNull(is);
          jos.putNextEntry(new JarEntry(entryName));
          int read;
          while ((read = is.read(buffer)) != -1) {
            jos.write(buffer, 0, read);
          }
          jos.closeEntry();
        }
      }
    }
    return locationFactory.create(jarFile.toURI());
  }

  @Test
  public void testCreateClassLoaderLoadsClassAndCleansUpOnClose() throws Exception {
    File dataDir = TEMP_FOLDER.newFolder("data");
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, dataDir.getAbsolutePath());

    File jarsDir = TEMP_FOLDER.newFolder("jars");
    Location artifactJar = createMinimalJar(
        new LocalLocationFactory(jarsDir),
        new File(jarsDir, "empty-art.jar"),
        EmptyClass.class,
        PluginTestRunnable.class);
    ArtifactInfo artifactInfo = new ArtifactInfo(
        "empty-art", "1.0.0", ArtifactScope.USER,
        ArtifactClasses.builder().build(), Collections.emptyMap());

    AbstractArtifactManager manager = new AbstractArtifactManager(cConf) {
      @Override
      protected Location getArtifactLocation(ArtifactInfo info, @Nullable String namespace) {
        return artifactJar;
      }

      @Override
      public List<ArtifactInfo> listArtifacts() {
        return Collections.singletonList(artifactInfo);
      }

      @Override
      public List<ArtifactInfo> listArtifacts(String namespace) {
        return Collections.singletonList(artifactInfo);
      }
    };

    File unpackRoot = new File(dataDir, cConf.get(Constants.AppFabric.TEMP_DIR));
    File unpackedDir;
    try (CloseableClassLoader classLoader = manager.createClassLoader(artifactInfo, null)) {
      Class<?> loaded = classLoader.loadClass(EmptyClass.class.getName());
      Assert.assertNotNull(loaded);
      Assert.assertNotSame(EmptyClass.class, loaded);

      File[] unpackedChildren = unpackRoot.listFiles();
      Assert.assertNotNull(unpackedChildren);
      Assert.assertEquals(1, unpackedChildren.length);
      File managerTmpDir = unpackedChildren[0];
      Assert.assertTrue(managerTmpDir.isDirectory());

      File[] classLoaderDirs = managerTmpDir.listFiles();
      Assert.assertNotNull(classLoaderDirs);
      Assert.assertEquals(1, classLoaderDirs.length);
      unpackedDir = classLoaderDirs[0];
      Assert.assertTrue(unpackedDir.isDirectory());
    }

    Assert.assertFalse("Unpacked artifact directory should be deleted on close",
        unpackedDir.exists());
  }
}
