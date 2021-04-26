/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.plugin;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class PluginInstantiatorTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  /**
   * If a plugin has some resources in main jar and in libraries, main jar should take precedence
   */
  @Test
  public void testResourceOrder() throws IOException {
    File appDir = TMP_FOLDER.newFolder();
    File pluginsDir = TMP_FOLDER.newFolder();
    File pluginDir = TMP_FOLDER.newFolder();
    File pluginArchive = TMP_FOLDER.newFile();
    File jarDir = TMP_FOLDER.newFolder();
    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
    CConfiguration cConf = CConfiguration.create();
    ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, appDir, this.getClass().getClassLoader());
    PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf,
                                                                   programClassLoader,
                                                                   pluginsDir);
    FileUtils.write(new File(jarDir, "test.class"), "jarData");
    FileUtils.write(new File(pluginDir, "test.class"), "pluginData");
    BundleJarUtil.createJar(jarDir, new File(pluginDir, "library.jar"));
    BundleJarUtil.createJar(pluginDir, pluginArchive);
    pluginInstantiator.addArtifact(Locations.toLocation(pluginArchive), artifactId);
    PluginClassLoader loader = pluginInstantiator.getPluginClassLoader(artifactId, Collections.emptyList());
    Assert.assertEquals("pluginData", IOUtils.toString(loader.getResource("test.class")));
    pluginInstantiator.close();
  }

}