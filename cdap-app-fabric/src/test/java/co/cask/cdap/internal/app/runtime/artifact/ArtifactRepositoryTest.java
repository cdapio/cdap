/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.common.conf.ArtifactConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.plugins.test.TestPlugin;
import co.cask.cdap.internal.app.plugins.test.TestPlugin2;
import co.cask.cdap.internal.app.runtime.artifact.app.PluginTestApp;
import co.cask.cdap.internal.app.runtime.artifact.app.PluginTestRunnable;
import co.cask.cdap.internal.app.runtime.artifact.plugin.EmptyClass;
import co.cask.cdap.internal.app.runtime.artifact.plugin.Plugin1;
import co.cask.cdap.internal.app.runtime.artifact.plugin.Plugin2;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Unit-tests for app template plugin support.
 */
public class ArtifactRepositoryTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final String TEST_EMPTY_CLASS = EmptyClass.class.getName();
  private static final Id.Artifact APP_ARTIFACT_ID =
    Id.Artifact.from(Id.Namespace.DEFAULT, "PluginTest", "1.0.0");

  private static CConfiguration cConf;
  private static File tmpDir;
  private static File systemArtifactsDir;
  private static ArtifactRepository artifactRepository;
  private static ClassLoader appClassLoader;

  @BeforeClass
  public static void setup() throws Exception {
    systemArtifactsDir = TMP_FOLDER.newFolder();
    tmpDir = TMP_FOLDER.newFolder();

    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR, systemArtifactsDir.getAbsolutePath());
    artifactRepository = AppFabricTestHelper.getInjector(cConf).getInstance(ArtifactRepository.class);
  }

  @Before
  public void setupData() throws Exception {
    artifactRepository.clear(Id.Namespace.DEFAULT);
    File appArtifactFile = createAppJar(PluginTestApp.class, new File(tmpDir, "PluginTest-1.0.0.jar"),
      createManifest(ManifestFields.EXPORT_PACKAGE, PluginTestRunnable.class.getPackage().getName()));
    artifactRepository.addArtifact(APP_ARTIFACT_ID, appArtifactFile, null);
    appClassLoader = createAppClassLoader(appArtifactFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMultipleParentVersions() throws InvalidArtifactException {
    Id.Artifact child = Id.Artifact.from(Id.Namespace.SYSTEM, "abc", "1.0.0");
    ArtifactRepository.validateParentSet(child, ImmutableSet.of(
      new ArtifactRange(Id.Namespace.SYSTEM, "r1", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")),
      new ArtifactRange(Id.Namespace.SYSTEM, "r1", new ArtifactVersion("3.0.0"), new ArtifactVersion("4.0.0"))));
  }

  @Test(expected = InvalidArtifactException.class)
  public void testSelfExtendingArtifact() throws InvalidArtifactException {
    Id.Artifact child = Id.Artifact.from(Id.Namespace.SYSTEM, "abc", "1.0.0");
    ArtifactRepository.validateParentSet(child, ImmutableSet.of(
      new ArtifactRange(Id.Namespace.SYSTEM, "abc", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))));
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMultiplePluginClasses() throws InvalidArtifactException {
    ArtifactRepository.validatePluginSet(ImmutableSet.of(
      new PluginClass("t1", "n1", "", "co.cask.test1", "cfg", ImmutableMap.<String, PluginPropertyField>of()),
      new PluginClass("t1", "n1", "", "co.cask.test2", "cfg", ImmutableMap.<String, PluginPropertyField>of())));
  }

  @Test
  public void testAddSystemArtifacts() throws Exception {
    Id.Artifact systemAppArtifactId = Id.Artifact.from(Id.Namespace.SYSTEM, "PluginTest", "1.0.0");
    createAppJar(PluginTestApp.class, new File(systemArtifactsDir, "PluginTest-1.0.0.jar"),
      createManifest(ManifestFields.EXPORT_PACKAGE, PluginTestRunnable.class.getPackage().getName()));

    // write plugins jar
    Id.Artifact pluginArtifactId = Id.Artifact.from(Id.Namespace.SYSTEM, "myPlugin", "1.0.0");

    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    createPluginJar(TestPlugin.class, new File(systemArtifactsDir, "myPlugin-1.0.0.jar"), manifest);

    // write plugins config file
    Map<String, PluginPropertyField> emptyMap = Collections.emptyMap();
    Set<PluginClass> manuallyAddedPlugins = ImmutableSet.of(
      new PluginClass("typeA", "manual1", "desc", "co.cask.classname", null, emptyMap),
      new PluginClass("typeB", "manual2", "desc", "co.cask.otherclassname", null, emptyMap)
    );
    File pluginConfigFile = new File(systemArtifactsDir, "myPlugin-1.0.0.json");
    ArtifactConfig pluginConfig = ArtifactConfig.builder(pluginArtifactId, pluginConfigFile)
      .addParents(new ArtifactRange(
        Id.Namespace.SYSTEM, "PluginTest", new ArtifactVersion("0.9.0"), new ArtifactVersion("2.0.0")))
      // add a dummy plugin to test explicit addition of plugins through the config file
      .addPlugins(manuallyAddedPlugins)
      .build();
    try (BufferedWriter writer = Files.newWriter(pluginConfigFile, Charsets.UTF_8)) {
      writer.write(pluginConfig.toString());
    }

    artifactRepository.addSystemArtifacts();

    // check app artifact added correctly
    ArtifactDetail appArtifactDetail = artifactRepository.getArtifact(systemAppArtifactId);
    Map<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(systemAppArtifactId);
    Assert.assertEquals(1, plugins.size());
    List<PluginClass> pluginClasses = plugins.values().iterator().next();

    Set<String> pluginNames = Sets.newHashSet();
    for (PluginClass pluginClass : pluginClasses) {
      pluginNames.add(pluginClass.getName());
    }
    Assert.assertEquals(Sets.newHashSet("manual1", "manual2", "TestPlugin", "TestPlugin2"), pluginNames);
    Assert.assertEquals(systemAppArtifactId.getName(), appArtifactDetail.getDescriptor().getArtifactId().getName());
    Assert.assertEquals(systemAppArtifactId.getVersion(),
                        appArtifactDetail.getDescriptor().getArtifactId().getVersion());

    // check plugin artifact added correctly
    ArtifactDetail pluginArtifactDetail = artifactRepository.getArtifact(pluginArtifactId);
    Assert.assertEquals(pluginArtifactId.getName(), pluginArtifactDetail.getDescriptor().getArtifactId().getName());
    Assert.assertEquals(pluginArtifactId.getVersion(),
                        pluginArtifactDetail.getDescriptor().getArtifactId().getVersion());
    // check manually added plugins are there
    Assert.assertTrue(pluginArtifactDetail.getMeta().getClasses().getPlugins().containsAll(manuallyAddedPlugins));
  }

  @Test
  public void testExportPackage() {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE,
      "co.cask.plugin;use:=\"\\\"test,test2\\\"\";version=\"1.0\",co.cask.plugin2");

    Set<String> packages = ManifestFields.getExportPackages(manifest);
    Assert.assertEquals(ImmutableSet.of("co.cask.plugin", "co.cask.plugin2"), packages);
  }

  @Test
  public void testPlugin() throws Exception {
    File pluginDir = DirUtils.createTempDir(tmpDir);
    // Create the plugin jar. There should be two plugins there (TestPlugin and TestPlugin2).
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);

    // add the artifact
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace(), APP_ARTIFACT_ID.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "1.0");
    artifactRepository.addArtifact(artifactId, jarFile, parents);

    // check the parent can see the plugins
    SortedMap<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(APP_ARTIFACT_ID);
    Assert.assertEquals(1, plugins.size());
    Assert.assertEquals(2, plugins.get(plugins.firstKey()).size());

    ArtifactDescriptor descriptor = plugins.firstKey();
    Files.copy(Locations.newInputSupplier(descriptor.getLocation()),
      new File(pluginDir, Artifacts.getFileName(descriptor.getArtifactId())));
    // Instantiate the plugins and execute them
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {
      for (Map.Entry<ArtifactDescriptor, List<PluginClass>> entry : plugins.entrySet()) {
        for (PluginClass pluginClass : entry.getValue()) {
          Plugin pluginInfo = new Plugin(entry.getKey().getArtifactId(), pluginClass,
                                         PluginProperties.builder().add("class.name", TEST_EMPTY_CLASS)
                                           .add("timeout", "10").build());
          Callable<String> plugin = instantiator.newInstance(pluginInfo);
          Assert.assertEquals(TEST_EMPTY_CLASS, plugin.call());
        }
      }
    }
  }

  @Test
  public void testPluginSelector() throws Exception {
    // No plugin yet
    try {
      artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector());
      Assert.fail();
    } catch (PluginNotExistsException e) {
      // expected
    }

    File pluginDir = DirUtils.createTempDir(tmpDir);

    // Create a plugin jar. It contains two plugins, TestPlugin and TestPlugin2 inside.
    Id.Artifact artifact1Id = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "1.0");
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace(), APP_ARTIFACT_ID.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(artifact1Id, jarFile, parents);

    // Should get the only version.
    Map.Entry<ArtifactDescriptor, PluginClass> plugin =
      artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("1.0"), plugin.getKey().getArtifactId().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());
    Files.copy(Locations.newInputSupplier(plugin.getKey().getLocation()),
               new File(pluginDir, Artifacts.getFileName(plugin.getKey().getArtifactId())));

    // Create another plugin jar with later version and update the repository
    Id.Artifact artifact2Id = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "2.0");
    jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-2.0.jar"), manifest);
    artifactRepository.addArtifact(artifact2Id, jarFile, parents);

    // Should select the latest version
    plugin = artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("2.0"), plugin.getKey().getArtifactId().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());
    Files.copy(Locations.newInputSupplier(plugin.getKey().getLocation()),
               new File(pluginDir, Artifacts.getFileName(plugin.getKey().getArtifactId())));

    // Load the Plugin class from the classLoader.
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {
      ClassLoader pluginClassLoader = instantiator.getArtifactClassLoader(plugin.getKey().getArtifactId());
      Class<?> pluginClass = pluginClassLoader.loadClass(TestPlugin2.class.getName());

      // Use a custom plugin selector to select with smallest version
      plugin = artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector() {
        @Override
        public Map.Entry<ArtifactId, PluginClass> select(SortedMap<ArtifactId, PluginClass> plugins) {
          return plugins.entrySet().iterator().next();
        }
      });
      Assert.assertNotNull(plugin);
      Assert.assertEquals(new ArtifactVersion("1.0"), plugin.getKey().getArtifactId().getVersion());
      Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

      // Load the Plugin class again from the current plugin selected
      // The plugin class should be different (from different ClassLoader)
      // The empty class should be the same (from the plugin lib ClassLoader)
      pluginClassLoader = instantiator.getArtifactClassLoader(plugin.getKey().getArtifactId());
      Assert.assertNotSame(pluginClass, pluginClassLoader.loadClass(TestPlugin2.class.getName()));

      // From the pluginClassLoader, loading export classes from the template jar should be allowed
      Class<?> cls = pluginClassLoader.loadClass(PluginTestRunnable.class.getName());
      // The class should be loaded from the parent artifact's classloader
      Assert.assertSame(appClassLoader.loadClass(PluginTestRunnable.class.getName()), cls);

      // From the plugin classloader, all cdap api classes is loadable as well.
      cls = pluginClassLoader.loadClass(Application.class.getName());
      // The Application class should be the same as the one in the system classloader
      Assert.assertSame(Application.class, cls);
    }
  }

  @Test(expected = InvalidArtifactException.class)
  public void testGrandparentsAreInvalid() throws Exception {
    // create child artifact
    Id.Artifact childId = Id.Artifact.from(Id.Namespace.DEFAULT, "child", "1.0.0");
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin1.class.getPackage().getName());
    File jarFile = createPluginJar(Plugin1.class, new File(tmpDir, "child-1.0.0.jar"), manifest);

    // add the artifact
    Set<ArtifactRange> parents = ImmutableSet.of(new ArtifactRange(
      APP_ARTIFACT_ID.getNamespace(), APP_ARTIFACT_ID.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(childId, jarFile, parents);

    // try and create grandchild, should throw exception
    Id.Artifact grandchildId = Id.Artifact.from(Id.Namespace.DEFAULT, "grandchild", "1.0.0");
    manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin2.class.getPackage().getName());
    jarFile = createPluginJar(Plugin2.class, new File(tmpDir, "grandchild-1.0.0.jar"), manifest);
    parents = ImmutableSet.of(new ArtifactRange(
      childId.getNamespace(), childId.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(grandchildId, jarFile, parents);
  }

  private static ClassLoader createAppClassLoader(File jarFile) throws IOException {
    final File unpackDir = DirUtils.createTempDir(TMP_FOLDER.newFolder());
    BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(jarFile), unpackDir);
    return ProgramClassLoader.create(unpackDir, ArtifactRepositoryTest.class.getClassLoader());
  }

  private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
      cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }

  private static File createPluginJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = PluginJarHelper.createPluginJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
      manifest, cls);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }

  private static Manifest createManifest(Object...entries) {
    Preconditions.checkArgument(entries.length % 2 == 0);
    Attributes attributes = new Attributes();
    for (int i = 0; i < entries.length; i += 2) {
      attributes.put(entries[i], entries[i + 1]);
    }
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);
    return manifest;
  }
}
