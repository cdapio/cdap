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
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.plugins.template.test.PluginTestAppTemplate;
import co.cask.cdap.internal.app.plugins.template.test.api.PluginTestRunnable;
import co.cask.cdap.internal.app.plugins.test.TestPlugin;
import co.cask.cdap.internal.app.plugins.test.TestPlugin2;
import co.cask.cdap.internal.app.runtime.adapter.EmptyClass;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.artifact.ArtifactVersion;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
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
    Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "PluginTest", "1.0.0");

  private static CConfiguration cConf;
  private static File tmpDir;
  private static ArtifactRepository artifactRepository;
  private static ClassLoader appClassLoader;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.APP_TEMPLATE_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    tmpDir = TMP_FOLDER.newFolder();
    artifactRepository = AppFabricTestHelper.getInjector(cConf).getInstance(ArtifactRepository.class);
  }

  @Before
  public void setupData() throws Exception {
    artifactRepository.clear(Constants.DEFAULT_NAMESPACE_ID);
    File appArtifactFile = createAppJar(PluginTestAppTemplate.class, new File(tmpDir, "PluginTest-1.0.0.jar"),
      createManifest(ManifestFields.EXPORT_PACKAGE,
        PluginTestRunnable.class.getPackage().getName()));
    artifactRepository.addArtifact(APP_ARTIFACT_ID, appArtifactFile, null);
    appClassLoader = createAppClassLoader(appArtifactFile);
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
    // Create the plugin jar. There should be two plugins there (TestPlugin and TestPlugin2).
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);

    // add the artifact
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace(), APP_ARTIFACT_ID.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myPlugin", "1.0");
    artifactRepository.addArtifact(artifactId, jarFile, parents);

    // check the parent can see the plugins
    SortedMap<ArtifactDescriptor, List<PluginClass>> plugins = artifactRepository.getPlugins(APP_ARTIFACT_ID);
    Assert.assertEquals(1, plugins.size());
    Assert.assertEquals(2, plugins.get(plugins.firstKey()).size());

    // Instantiate the plugins and execute them
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader)) {
      for (Map.Entry<ArtifactDescriptor, List<PluginClass>> entry : plugins.entrySet()) {
        for (PluginClass pluginClass : entry.getValue()) {
          Callable<String> plugin = instantiator.newInstance(entry.getKey(), pluginClass,
                                                             PluginProperties.builder()
                                                              .add("class.name", TEST_EMPTY_CLASS)
                                                              .add("timeout", "10")
                                                              .build()
          );
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

    // Create a plugin jar. It contains two plugins, TestPlugin and TestPlugin2 inside.
    Id.Artifact artifact1Id = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myPlugin", "1.0");
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
    Assert.assertEquals(new ArtifactVersion("1.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Create another plugin jar with later version and update the repository
    Id.Artifact artifact2Id = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "myPlugin", "2.0");
    jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-2.0.jar"), manifest);
    artifactRepository.addArtifact(artifact2Id, jarFile, parents);

    // Should select the latest version
    plugin = artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("2.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Load the Plugin class from the classLoader.
    PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader);
    ClassLoader pluginClassLoader = instantiator.getArtifactClassLoader(plugin.getKey());
    Class<?> pluginClass = pluginClassLoader.loadClass(TestPlugin2.class.getName());

    // Use a custom plugin selector to select with smallest version
    plugin = artifactRepository.findPlugin(APP_ARTIFACT_ID, "plugin", "TestPlugin2", new PluginSelector() {
      @Override
      public Map.Entry<ArtifactDescriptor, PluginClass> select(SortedMap<ArtifactDescriptor, PluginClass> plugins) {
        return plugins.entrySet().iterator().next();
      }
    });
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("1.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Load the Plugin class again from the current plugin selected
    // The plugin class should be different (from different ClassLoader)
    // The empty class should be the same (from the plugin lib ClassLoader)
    pluginClassLoader = instantiator.getArtifactClassLoader(plugin.getKey());
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
