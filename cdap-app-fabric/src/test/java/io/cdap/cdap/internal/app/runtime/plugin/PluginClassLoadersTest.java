/*
 * Copyright © 2025 Cask Data, Inc.
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
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link PluginClassLoaders}.
 */
public class PluginClassLoadersTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCreateFilteredPluginsClassLoaderEmptyOrNull() throws Exception {
    ClassLoader emptyLoader = PluginClassLoaders.createFilteredPluginsClassLoader(
        Collections.emptyMap(), null);
    Assert.assertTrue(emptyLoader instanceof CombineClassLoader);

    ArtifactId artifactId = new ArtifactId("p", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    Plugin plugin = createPlugin(artifactId, "allowed.AllowedPlugin");
    ClassLoader nullInstantiatorLoader = PluginClassLoaders.createFilteredPluginsClassLoader(
        Collections.singletonMap("id1", plugin), null);
    Assert.assertTrue(nullInstantiatorLoader instanceof CombineClassLoader);
  }

  @Test
  public void testCreateFilteredPluginsClassLoaderFiltersResources() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TEMP_FOLDER.newFolder();
    File jarContents = TEMP_FOLDER.newFolder();

    File allowedDir = new File(jarContents, "allowed");
    File blockedDir = new File(jarContents, "blocked");
    Assert.assertTrue(allowedDir.mkdirs());
    Assert.assertTrue(blockedDir.mkdirs());
    Files.write(
        new File(allowedDir, "AllowedPlugin.class").toPath(),
        "allowed".getBytes(StandardCharsets.UTF_8));
    Files.write(
        new File(blockedDir, "InternalHelper.class").toPath(),
        "blocked".getBytes(StandardCharsets.UTF_8));

    File pluginJar = TEMP_FOLDER.newFile("plugin.jar");
    BundleJarUtil.createJar(jarContents, pluginJar);
    ArtifactId artifactId = new ArtifactId("plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    Plugin plugin = createPlugin(artifactId, "allowed.AllowedPlugin");

    try (PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false)) {
      instantiator.addArtifact(Locations.toLocation(pluginJar), artifactId);
      ClassLoader filtered = PluginClassLoaders.createFilteredPluginsClassLoader(
          Collections.singletonMap("p1", plugin), instantiator);

      Assert.assertNotNull(filtered.getResource("allowed/AllowedPlugin.class"));
      Assert.assertNull(filtered.getResource("blocked/InternalHelper.class"));
    }
  }

  @Test
  public void testCreateFilteredPluginsClassLoaderWrapsIoException() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TEMP_FOLDER.newFolder();
    IOException expectedIo = new IOException("corrupt plugin archive");
    ArtifactId artifactId = new ArtifactId("plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    Plugin plugin = createPlugin(artifactId, "allowed.AllowedPlugin");

    try (PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false) {
          @Override
          public ClassLoader getPluginClassLoader(Plugin target) throws IOException {
            throw expectedIo;
          }
        }) {
      try {
        PluginClassLoaders.createFilteredPluginsClassLoader(
            Collections.singletonMap("p1", plugin), instantiator);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(expectedIo, thrown.getCause());
      }
    }
  }

  private Plugin createPlugin(ArtifactId artifactId, String className) {
    PluginClass pluginClass = PluginClass.builder()
        .setName("pluginName")
        .setType("transform")
        .setDescription("desc")
        .setClassName(className)
        .build();
    return new Plugin(
        Collections.emptyList(), artifactId, pluginClass, PluginProperties.builder().build());
  }
}
