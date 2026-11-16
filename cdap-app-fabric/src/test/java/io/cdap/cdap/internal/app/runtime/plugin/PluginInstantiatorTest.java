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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link PluginInstantiator}.
 */
public class PluginInstantiatorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static volatile Throwable nextConstructorFailure;

  /**
   * Test config used for constructor exception propagation tests.
   */
  public static class ConstructorTestConfig extends PluginConfig {
    private String mode;
  }

  /**
   * Test plugin whose single-arg config constructor throws {@link #nextConstructorFailure}.
   */
  public static class ThrowingConstructorPlugin {
    private final ConstructorTestConfig config;

    /**
     * Constructs the plugin or throws the configured test failure.
     */
    public ThrowingConstructorPlugin(ConstructorTestConfig config) throws Exception {
      this.config = config;
      Throwable failure = nextConstructorFailure;
      if (failure instanceof Exception) {
        throw (Exception) failure;
      }
      if (failure instanceof Error) {
        throw (Error) failure;
      }
    }

    public String getMode() {
      return config.mode;
    }
  }

  /**
   * If a plugin has some resources in main jar and in libraries, main jar should take precedence.
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
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    ProgramClassLoader programClassLoader =
        new ProgramClassLoader(cConf, appDir, this.getClass().getClassLoader());
    PluginInstantiator pluginInstantiator =
        new PluginInstantiator(cConf, programClassLoader, pluginsDir);
    FileUtils.write(new File(jarDir, "test.class"), "jarData", StandardCharsets.UTF_8);
    FileUtils.write(new File(pluginDir, "test.class"), "pluginData", StandardCharsets.UTF_8);
    BundleJarUtil.createJar(jarDir, new File(pluginDir, "library.jar"));
    BundleJarUtil.createJar(pluginDir, pluginArchive);
    pluginInstantiator.addArtifact(Locations.toLocation(pluginArchive), artifactId);
    PluginClassLoader loader =
        pluginInstantiator.getPluginClassLoader(artifactId, Collections.emptyList());
    Assert.assertEquals(
        "pluginData", IOUtils.toString(loader.getResource("test.class"), StandardCharsets.UTF_8));
    pluginInstantiator.close();
  }

  @Test
  public void testSubstituteMacros() throws Exception {
    File appDir = TMP_FOLDER.newFolder();
    File pluginsDir = TMP_FOLDER.newFolder();
    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    ProgramClassLoader programClassLoader =
        new ProgramClassLoader(cConf, appDir, this.getClass().getClassLoader());
    try (PluginInstantiator pluginInstantiator =
        new PluginInstantiator(cConf, programClassLoader, pluginsDir)) {
      PluginClass pluginClass = PluginClass.builder()
          .setClassName("")
          .setName("")
          .setCategory("")
          .setConfigFieldName("")
          .setRequirements(Requirements.EMPTY)
          .setType("")
          .setDescription("")
          .add("key1", new PluginPropertyField("key1", "", "string", false, true))
          .add("key2", new PluginPropertyField("key2", "", "Connection", false, true, false,
              ImmutableSet.of("child1", "child2")))
          .add("child1", new PluginPropertyField("child1", "", "string", false, true))
          .add("child2", new PluginPropertyField("child2", "", "string", false, true))
          .build();
      Gson gson = new Gson();

      Map<String, String> childProperties =
          ImmutableMap.of("child1", "childVal1", "child2", "${secure(acc)}", "child3", "val3");
      Map<String, String> properties =
          ImmutableMap.of("key2", gson.toJson(childProperties), "key1", "val1");

      Plugin plugin = new Plugin(
          Collections.emptyList(), artifactId, pluginClass,
          PluginProperties.builder().addAll(properties).build());

      PluginProperties pluginProperties = pluginInstantiator.substituteMacros(plugin, null, null);
      Map<String, String> expectedChildProperties = new HashMap<>();
      expectedChildProperties.put("child1", "childVal1");
      expectedChildProperties.put("child2", null);
      Assert.assertEquals(
          PluginProperties.builder().addAll(
              ImmutableMap.of("key1", "val1", "key2", gson.toJson(expectedChildProperties))).build(),
          pluginProperties);
    }
  }

  @Test
  public void testNewInstancePropagatesInvalidPluginConfigExceptionUnwrapped() throws Exception {
    InvalidPluginConfigException expected =
        new InvalidPluginConfigException("invalid config", new IllegalArgumentException("bad"));
    nextConstructorFailure = expected;
    try (PluginInstantiator instantiator = createUnfilteredInstantiatorWithEmptyPluginJar("p1")) {
      Plugin plugin = createThrowingConstructorPlugin("p1");
      try {
        instantiator.newInstance(plugin);
        Assert.fail("Expected InvalidPluginConfigException");
      } catch (InvalidPluginConfigException thrown) {
        Assert.assertSame(expected, thrown);
      }
    } finally {
      nextConstructorFailure = null;
    }
  }

  @Test
  public void testNewInstancePropagatesRuntimeExceptionUnwrapped() throws Exception {
    IllegalArgumentException expected = new IllegalArgumentException("bad argument");
    nextConstructorFailure = expected;
    try (PluginInstantiator instantiator = createUnfilteredInstantiatorWithEmptyPluginJar("p2")) {
      Plugin plugin = createThrowingConstructorPlugin("p2");
      try {
        instantiator.newInstance(plugin);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException thrown) {
        Assert.assertSame(expected, thrown);
      }
    } finally {
      nextConstructorFailure = null;
    }
  }

  @Test
  public void testNewInstanceWrapsCheckedConstructorException() throws Exception {
    IOException expected = new IOException("checked failure in plugin ctor");
    nextConstructorFailure = expected;
    try (PluginInstantiator instantiator = createUnfilteredInstantiatorWithEmptyPluginJar("p3")) {
      Plugin plugin = createThrowingConstructorPlugin("p3");
      try {
        instantiator.newInstance(plugin);
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException thrown) {
        Assert.assertSame(expected, thrown.getCause());
      }
    } finally {
      nextConstructorFailure = null;
    }
  }

  @Test
  public void testClassLoaderCacheIoExceptionAndCloseCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File localDataDir = TMP_FOLDER.newFolder();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    File pluginsDir = TMP_FOLDER.newFolder();

    ArtifactId missingArtifact =
        new ArtifactId("missing", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactId validArtifact =
        new ArtifactId("valid", new ArtifactVersion("1.0.0"), ArtifactScope.USER);

    File emptyJarSource = TMP_FOLDER.newFolder();
    FileUtils.write(new File(emptyJarSource, "res.txt"), "hello", StandardCharsets.UTF_8);
    File validArchive = TMP_FOLDER.newFile("valid-1.0.0.jar");
    BundleJarUtil.createJar(emptyJarSource, validArchive);

    ProgramClassLoader parentClassLoader =
        new ProgramClassLoader(cConf, TMP_FOLDER.newFolder(), getClass().getClassLoader());
    PluginInstantiator instantiator =
        new PluginInstantiator(cConf, parentClassLoader, pluginsDir, true);
    try {
      instantiator.getArtifactClassLoader(missingArtifact);
      Assert.fail("Expected IOException for missing artifact");
    } catch (IOException expected) {
      // expected direct IOException, not wrapped in RuntimeException or ExecutionException
    }

    try {
      instantiator.getPluginClassLoader(missingArtifact, Collections.emptyList());
      Assert.fail("Expected IOException for missing plugin artifact");
    } catch (IOException expected) {
      // expected direct IOException
    }

    instantiator.addArtifact(Locations.toLocation(validArchive), validArtifact);
    PluginClassLoader artifactClassLoader = instantiator.getArtifactClassLoader(validArtifact);
    Assert.assertNotNull(artifactClassLoader.getResource("res.txt"));

    // Closing invalidates the cache (closing cached PluginClassLoader via RemovalListener)
    // and closes ownedParentClassLoader quietly.
    instantiator.close();
    instantiator.close();
  }

  private PluginInstantiator createUnfilteredInstantiatorWithEmptyPluginJar(String artifactName)
      throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    File pluginsDir = TMP_FOLDER.newFolder();
    File jarSource = TMP_FOLDER.newFolder();
    FileUtils.write(new File(jarSource, "marker.txt"), "ok", StandardCharsets.UTF_8);
    File pluginJar = TMP_FOLDER.newFile(artifactName + "-1.0.0.jar");
    BundleJarUtil.createJar(jarSource, pluginJar);

    ArtifactId artifactId =
        new ArtifactId(artifactName, new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    PluginInstantiator instantiator =
        new PluginInstantiator(cConf, getClass().getClassLoader(), pluginsDir, false);
    instantiator.addArtifact(Locations.toLocation(pluginJar), artifactId);
    return instantiator;
  }

  private Plugin createThrowingConstructorPlugin(String artifactName) {
    ArtifactId artifactId =
        new ArtifactId(artifactName, new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    PluginClass pluginClass = PluginClass.builder()
        .setName("throwing")
        .setType("transform")
        .setDescription("desc")
        .setClassName(ThrowingConstructorPlugin.class.getName())
        .setConfigFieldName("config")
        .add("mode", new PluginPropertyField("mode", "mode", "string", false, false))
        .build();
    return new Plugin(
        Collections.emptyList(),
        artifactId,
        pluginClass,
        PluginProperties.builder().add("mode", "strict").build());
  }
}
