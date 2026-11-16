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
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link FindPluginHelper}.
 */
public class FindPluginHelperTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGetPluginAddsArtifactAndCollectsMacros() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TEMP_FOLDER.newFolder();
    File jarSourceDir = TEMP_FOLDER.newFolder();
    Files.write(new File(jarSourceDir, "dummy.txt").toPath(), "ok".getBytes(StandardCharsets.UTF_8));
    File pluginJar = TEMP_FOLDER.newFile("my-plugin.jar");
    BundleJarUtil.createJar(jarSourceDir, pluginJar);

    ArtifactId parentId = new ArtifactId("parent", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactId pluginArtifactId = new ArtifactId("my-plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        "default", pluginArtifactId, Locations.toLocation(pluginJar));
    PluginClass pluginClass = PluginClass.builder()
        .setName("myPlugin")
        .setType("transform")
        .setDescription("desc")
        .setClassName("com.example.MyPlugin")
        .add("host", new PluginPropertyField("host", "host", "string", true, true))
        .build();
    Map.Entry<ArtifactDescriptor, PluginClass> entry =
        new AbstractMap.SimpleImmutableEntry<>(descriptor, pluginClass);
    PluginProperties props = PluginProperties.builder().add("host", "${runtimeHost}").build();

    try (PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false)) {
      Plugin plugin = FindPluginHelper.getPlugin(
          Collections.singletonList(parentId), entry, props, instantiator);
      Assert.assertEquals(pluginArtifactId, plugin.getArtifactId());
      Assert.assertEquals(Collections.singletonList(parentId), plugin.getParents());
      Assert.assertTrue(plugin.getProperties().getMacros().getLookups().contains("runtimeHost"));
    }
  }

  @Test
  public void testGetPluginWrapsIoExceptionFromAddArtifact() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TEMP_FOLDER.newFolder();
    IOException expectedIo = new IOException("failed to copy artifact");

    ArtifactId pluginArtifactId = new ArtifactId("my-plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        "default", pluginArtifactId, Locations.toLocation(TEMP_FOLDER.newFile()));
    PluginClass pluginClass = PluginClass.builder()
        .setName("myPlugin")
        .setType("transform")
        .setDescription("desc")
        .setClassName("com.example.MyPlugin")
        .build();
    Map.Entry<ArtifactDescriptor, PluginClass> entry =
        new AbstractMap.SimpleImmutableEntry<>(descriptor, pluginClass);

    try (PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false) {
          @Override
          public void addArtifact(Location artifactLocation, ArtifactId destArtifact) throws IOException {
            throw expectedIo;
          }
        }) {
      try {
        FindPluginHelper.getPlugin(
            Collections.emptyList(), entry, PluginProperties.builder().build(), instantiator);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(expectedIo, thrown.getCause());
      }
    }
  }

  @Test
  public void testGetPluginPropagatesRuntimeExceptionUnwrapped() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TEMP_FOLDER.newFolder();
    IllegalStateException expectedRuntime = new IllegalStateException("unrecoverable state");

    ArtifactId pluginArtifactId = new ArtifactId("my-plugin", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        "default", pluginArtifactId, Locations.toLocation(TEMP_FOLDER.newFile()));
    PluginClass pluginClass = PluginClass.builder()
        .setName("myPlugin")
        .setType("transform")
        .setDescription("desc")
        .setClassName("com.example.MyPlugin")
        .build();
    Map.Entry<ArtifactDescriptor, PluginClass> entry =
        new AbstractMap.SimpleImmutableEntry<>(descriptor, pluginClass);

    try (PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false) {
          @Override
          public void addArtifact(Location artifactLocation, ArtifactId destArtifact) {
            throw expectedRuntime;
          }
        }) {
      try {
        FindPluginHelper.getPlugin(
            Collections.emptyList(), entry, PluginProperties.builder().build(), instantiator);
        Assert.fail("Expected IllegalStateException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(expectedRuntime, thrown);
      }
    }
  }
}
