/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.ArtifactConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.plugins.test.TestPlugin;
import io.cdap.cdap.internal.app.plugins.test.TestPlugin2;
import io.cdap.cdap.internal.app.runtime.DefaultPluginContext;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.EmptyClass;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.Plugin1;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.Plugin2;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.nested.NestedConfigPlugin;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.app.runtime.plugin.TestMacroEvaluator;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.Read;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Unit-tests for app template plugin support.
 */
public class ArtifactRepositoryTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final String TEST_EMPTY_CLASS = EmptyClass.class.getName();
  private static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "PluginTest", "1.0.0");

  private static CConfiguration cConf;
  private static File tmpDir;
  private static File systemArtifactsDir1;
  private static File systemArtifactsDir2;
  private static ArtifactRepository artifactRepository;
  private static ProgramClassLoader appClassLoader;
  private static MetadataStorage metadataStorage;
  private static File appArtifactFile;
  private static MetadataAdmin metadataAdmin;

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  @BeforeClass
  public static void setup() throws Exception {
    systemArtifactsDir1 = TMP_FOLDER.newFolder();
    systemArtifactsDir2 = TMP_FOLDER.newFolder();
    tmpDir = TMP_FOLDER.newFolder();

    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR,
              systemArtifactsDir1.getAbsolutePath() + ";" + systemArtifactsDir2.getAbsolutePath());
    Injector injector =  AppFabricTestHelper.getInjector(cConf);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
    metadataStorage = injector.getInstance(MetadataStorage.class);

    appArtifactFile = createAppJar(PluginTestApp.class, new File(tmpDir, "PluginTest-1.0.0.jar"),
                                   createManifest(ManifestFields.EXPORT_PACKAGE,
                                                  PluginTestRunnable.class.getPackage().getName()));
    metadataAdmin = injector.getInstance(MetadataAdmin.class);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Before
  public void setupData() throws Exception {
    artifactRepository.clear(NamespaceId.DEFAULT);
    artifactRepository.addArtifact(APP_ARTIFACT_ID, appArtifactFile, null, null);
    appClassLoader = createAppClassLoader(appArtifactFile);
  }

  @After
  public void cleanup() throws IOException {
    File unpackedDir = appClassLoader.getDir();
    try {
      appClassLoader.close();
    } finally {
      DirUtils.deleteDirectoryContents(unpackedDir);
    }
  }

  @Test
  public void testDeletingArtifact() throws Exception {
    Tasks.waitFor(false,
                  () -> metadataStorage.read(new Read(
                    APP_ARTIFACT_ID.toEntityId().toMetadataEntity(), MetadataScope.SYSTEM)).getProperties().isEmpty(),
                  5, TimeUnit.SECONDS);
    artifactRepository.deleteArtifact(APP_ARTIFACT_ID);
    Tasks.waitFor(true,
                  () -> metadataStorage.read(new Read(
                    APP_ARTIFACT_ID.toEntityId().toMetadataEntity(), MetadataScope.SYSTEM)).getProperties().isEmpty(),
                  5, TimeUnit.SECONDS);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMultipleParentVersions() throws InvalidArtifactException {
    Id.Artifact child = Id.Artifact.from(Id.Namespace.SYSTEM, "abc", "1.0.0");
    DefaultArtifactRepository.validateParentSet(child, ImmutableSet.of(
      new ArtifactRange(NamespaceId.SYSTEM.getNamespace(), "r1", new ArtifactVersion("1.0.0"),
                        new ArtifactVersion("2.0.0")),
      new ArtifactRange(NamespaceId.SYSTEM.getNamespace(), "r1",
                        new ArtifactVersion("3.0.0"), new ArtifactVersion("4.0.0"))));
  }

  @Test(expected = InvalidArtifactException.class)
  public void testSelfExtendingArtifact() throws InvalidArtifactException {
    Id.Artifact child = Id.Artifact.from(Id.Namespace.SYSTEM, "abc", "1.0.0");
    DefaultArtifactRepository.validateParentSet(child, ImmutableSet.of(
      new ArtifactRange(NamespaceId.SYSTEM.getNamespace(), "abc",
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))));
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMultiplePluginClasses() throws InvalidArtifactException {
    DefaultArtifactRepository.validatePluginSet(ImmutableSet.of(
      PluginClass.builder().setName("n1").setType("t1").setDescription("").setClassName("io.cdap.test1")
        .setConfigFieldName("cfg").setProperties(ImmutableMap.of()).build(),
      PluginClass.builder().setName("n1").setType("t1").setDescription("").setClassName("io.cdap.test2")
        .setConfigFieldName("cfg").setProperties(ImmutableMap.of()).build()));
  }

  @Test
  public void testAddSystemArtifacts() throws Exception {
    Id.Artifact systemAppArtifactId = Id.Artifact.from(Id.Namespace.SYSTEM, "PluginTest", "1.0.0");
    File systemAppJar = createAppJar(PluginTestApp.class, new File(systemArtifactsDir1, "PluginTest-1.0.0.jar"),
                                     createManifest(ManifestFields.EXPORT_PACKAGE,
                                                    PluginTestRunnable.class.getPackage().getName()));

    // write plugins jar
    Id.Artifact pluginArtifactId1 = Id.Artifact.from(Id.Namespace.SYSTEM, "APlugin", "1.0.0");

    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File pluginJar1 = createPluginJar(TestPlugin.class, new File(systemArtifactsDir1, "APlugin-1.0.0.jar"), manifest);

    // write plugins config file
    Map<String, PluginPropertyField> emptyMap = Collections.emptyMap();
    Set<PluginClass> manuallyAddedPlugins1 = ImmutableSet.of(
      PluginClass.builder().setName("manual1").setType("typeA").setDescription("desc")
        .setClassName(TestPlugin.class.getName()).setProperties(emptyMap).build(),
      PluginClass.builder().setName("manual2").setType("typeB").setDescription("desc")
        .setClassName(TestPlugin.class.getName()).setProperties(emptyMap).build()
    );
    File pluginConfigFile = new File(systemArtifactsDir1, "APlugin-1.0.0.json");
    ArtifactConfig pluginConfig1 = new ArtifactConfig(
      ImmutableSet.of(new ArtifactRange(
        NamespaceId.SYSTEM.getNamespace(), "PluginTest", new ArtifactVersion("0.9.0"), new ArtifactVersion("2.0.0"))),
      // add a dummy plugin to test explicit addition of plugins through the config file
      manuallyAddedPlugins1,
      ImmutableMap.of("k1", "v1", "k2", "v2")
    );
    try (BufferedWriter writer = Files.newWriter(pluginConfigFile, Charsets.UTF_8)) {
      writer.write(pluginConfig1.toString());
    }

    // write another plugins jar to a different directory, to test that plugins will get picked up from both directories
    Id.Artifact pluginArtifactId2 = Id.Artifact.from(Id.Namespace.SYSTEM, "BPlugin", "1.0.0");

    manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File pluginJar2 = createPluginJar(TestPlugin.class, new File(systemArtifactsDir2, "BPlugin-1.0.0.jar"), manifest);

    // write plugins config file
    Set<PluginClass> manuallyAddedPlugins2 = ImmutableSet.of(
      PluginClass.builder().setName("manual1").setType("typeA").setDescription("desc")
        .setClassName(TestPlugin.class.getName()).setProperties(emptyMap).build(),
      PluginClass.builder().setName("manual2").setType("typeB").setDescription("desc")
        .setClassName(TestPlugin.class.getName()).setProperties(emptyMap).build()
    );
    pluginConfigFile = new File(systemArtifactsDir2, "BPlugin-1.0.0.json");
    ArtifactConfig pluginConfig2 = new ArtifactConfig(
      ImmutableSet.of(new ArtifactRange(
        NamespaceId.SYSTEM.getNamespace(), "PluginTest", new ArtifactVersion("0.9.0"), new ArtifactVersion("2.0.0"))),
      manuallyAddedPlugins2,
      ImmutableMap.of("k3", "v3")
    );
    try (BufferedWriter writer = Files.newWriter(pluginConfigFile, Charsets.UTF_8)) {
      writer.write(pluginConfig2.toString());
    }

    artifactRepository.addSystemArtifacts();
    Assert.assertTrue(systemAppJar.delete());
    Assert.assertTrue(pluginJar1.delete());
    Assert.assertTrue(pluginJar2.delete());

    try {
      // check app artifact added correctly
      ArtifactDetail appArtifactDetail = artifactRepository.getArtifact(systemAppArtifactId);
      Map<ArtifactDescriptor, Set<PluginClass>> plugins =
        artifactRepository.getPlugins(NamespaceId.DEFAULT, systemAppArtifactId);
      Assert.assertEquals(2, plugins.size());
      Set<PluginClass> pluginClasses = plugins.values().iterator().next();

      Set<String> pluginNames = Sets.newHashSet();
      for (PluginClass pluginClass : pluginClasses) {
        pluginNames.add(pluginClass.getName());
      }
      Assert.assertEquals(Sets.newHashSet("manual1", "manual2", "TestPlugin", "TestPlugin2"), pluginNames);
      Assert.assertEquals(systemAppArtifactId.getName(), appArtifactDetail.getDescriptor().getArtifactId().getName());
      Assert.assertEquals(systemAppArtifactId.getVersion(),
                          appArtifactDetail.getDescriptor().getArtifactId().getVersion());

      // check plugin artifact added correctly
      ArtifactDetail pluginArtifactDetail = artifactRepository.getArtifact(pluginArtifactId1);
      Assert.assertEquals(pluginArtifactId1.getName(), pluginArtifactDetail.getDescriptor().getArtifactId().getName());
      Assert.assertEquals(pluginArtifactId1.getVersion(),
                          pluginArtifactDetail.getDescriptor().getArtifactId().getVersion());
      // check manually added plugins are there
      Assert.assertTrue(pluginArtifactDetail.getMeta().getClasses().getPlugins().containsAll(manuallyAddedPlugins1));
      // check properties are there
      Assert.assertEquals(pluginConfig1.getProperties(), pluginArtifactDetail.getMeta().getProperties());

      // check other plugin artifact added correctly
      pluginArtifactDetail = artifactRepository.getArtifact(pluginArtifactId2);
      Assert.assertEquals(pluginArtifactId2.getName(), pluginArtifactDetail.getDescriptor().getArtifactId().getName());
      Assert.assertEquals(pluginArtifactId2.getVersion(),
                          pluginArtifactDetail.getDescriptor().getArtifactId().getVersion());
      // check manually added plugins are there
      Assert.assertTrue(pluginArtifactDetail.getMeta().getClasses().getPlugins().containsAll(manuallyAddedPlugins2));
      // check properties are there
      Assert.assertEquals(pluginConfig2.getProperties(), pluginArtifactDetail.getMeta().getProperties());
    } finally {
      artifactRepository.clear(NamespaceId.SYSTEM);
    }
  }

  @Test
  public void testExportPackage() {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE,
                                     "io.cdap.plugin;use:=\"\\\"test,test2\\\"\";version=\"1.0\",io.cdap.plugin2");

    Set<String> packages = ManifestFields.getExportPackages(manifest);
    Assert.assertEquals(ImmutableSet.of("io.cdap.plugin", "io.cdap.plugin2"), packages);
  }

  @Test
  public void testPlugin() throws Exception {
    File pluginDir = TMP_FOLDER.newFolder();
    addPluginArtifact();
    SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins = getPlugins();
    copyArtifacts(pluginDir, plugins);

    // Instantiate the plugins and execute them
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {
      for (Map.Entry<ArtifactDescriptor, Set<PluginClass>> entry : plugins.entrySet()) {
        for (PluginClass pluginClass : entry.getValue()) {
          Plugin pluginInfo = new Plugin(new ArrayList<>(), entry.getKey().getArtifactId(), pluginClass,
                                         PluginProperties.builder().add("class.name", TEST_EMPTY_CLASS)
                                           .add("nullableLongFlag", "10")
                                           .add("host", "example.com")
                                           .add("aBoolean", "${aBoolean}")
                                           .add("aByte", "${aByte}")
                                           .add("aChar", "${aChar}")
                                           .add("aDouble", "${aDouble}")
                                           .add("anInt", "${anInt}")
                                           .add("aFloat", "${aFloat}")
                                           .add("aLong", "${aLong}")
                                           .add("aShort", "${aShort}")
                                           .build());
          Callable<String> plugin = instantiator.newInstance(pluginInfo);
          Assert.assertEquals("example.com,false,0,\u0000,0.0,0.0,0,0,0,null", plugin.call());
        }
      }
    }
  }

  @Test
  public void testInstantiateNestedConfigPlugins() throws Exception {
    File pluginDir = TMP_FOLDER.newFolder();
    // Create the plugin jar.
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, NestedConfigPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(NestedConfigPlugin.class, new File(tmpDir, "nested-1.0.jar"), manifest);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "nested", "1.0");
    artifactRepository.addArtifact(
      artifactId, jarFile,
      Collections.singleton(new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
                                              new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))), null);
    SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins =
      artifactRepository.getPlugins(NamespaceId.DEFAULT, APP_ARTIFACT_ID);
    Assert.assertEquals(1, plugins.keySet().size());
    Assert.assertEquals(1, plugins.values().size());
    Assert.assertEquals(1, plugins.values().iterator().next().size());
    copyArtifacts(pluginDir, plugins);

    Gson gson = new Gson();
    ArtifactId artifact = plugins.firstKey().getArtifactId();
    // only 1 plugin
    PluginClass pluginClass = plugins.values().iterator().next().iterator().next();
    NestedConfigPlugin.Config expected =
      new NestedConfigPlugin.Config(1, new NestedConfigPlugin.NestedConfig("nested val1", "nested val2"));

    // test flat structure, all pre 6.5 plugins look like this
    instantiateAndValidate(pluginDir, new Plugin(Collections.emptyList(), artifact, pluginClass,
                                                 PluginProperties.builder()
                                                   .add("X", "1")
                                                   .add("Nested1", "nested val1")
                                                   .add("Nested2", "nested val2").build()), expected, false);

    // test nested
    instantiateAndValidate(pluginDir, new Plugin(Collections.emptyList(), artifact, pluginClass,
                                                 PluginProperties.builder()
                                                   .add("X", "1")
                                                   .add("Nested",
                                                        gson.toJson(ImmutableMap.of("Nested1", "nested val1",
                                                                                    "Nested2", "nested val2")))
                                                   .build()), expected, false);

    // test macro gets correct default value, and macro fields get set correctly
    expected = new NestedConfigPlugin.Config(1, new NestedConfigPlugin.NestedConfig(null, null));
    instantiateAndValidate(pluginDir, new Plugin(Collections.emptyList(), artifact, pluginClass,
                                                 PluginProperties.builder()
                                                   .add("X", "1")
                                                   .add("Nested1", "${macro1}")
                                                   .add("Nested2", "${macro2}").build()), expected, true);
    instantiateAndValidate(pluginDir, new Plugin(Collections.emptyList(), artifact, pluginClass,
                                                 PluginProperties.builder()
                                                   .add("X", "1")
                                                   .add("Nested", "${I am macro}")
                                                   .build()), expected, true);
  }

  private void instantiateAndValidate(File pluginDir, Plugin pluginInfo,
                                      NestedConfigPlugin.Config expected, boolean containMacro) throws Exception {
    Gson gson = new Gson();
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {

      // here cannot use directly cast to NestedConfigPlugin since the classloaders are different, will get
      // ClassCastException
      Callable<String> plugin = instantiator.newInstance(pluginInfo);
      NestedConfigPlugin.Config actual = gson.fromJson(plugin.call(), NestedConfigPlugin.Config.class);
      Assert.assertEquals(expected, actual);
      if (containMacro) {
        Assert.assertTrue(actual.nested.containsMacro("Nested1"));
        Assert.assertTrue(actual.nested.containsMacro("Nested2"));
      } else {
        Assert.assertFalse(actual.nested.containsMacro("Nested1"));
        Assert.assertFalse(actual.nested.containsMacro("Nested2"));
      }
    }
  }

  @Test
  public void testPluginConfigWithNoStringValues() throws Exception {
    File pluginDir = TMP_FOLDER.newFolder();
    addPluginArtifact();
    SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins = getPlugins();
    copyArtifacts(pluginDir, plugins);

    String numericValue = "42";

    // Instantiate the plugins and execute them
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {
      for (Map.Entry<ArtifactDescriptor, Set<PluginClass>> entry : plugins.entrySet()) {
        for (PluginClass pluginClass : entry.getValue()) {
          Plugin pluginInfo = new Plugin(new ArrayList<>(), entry.getKey().getArtifactId(), pluginClass,
                                         PluginProperties.builder().add("class.name", TEST_EMPTY_CLASS)
                                           .add("nullableLongFlag", numericValue)
                                           .add("host", "example.com")
                                           .add("aBoolean", "${aBoolean}")
                                           .add("aByte", numericValue)
                                           .add("aChar", "${aChar}")
                                           .add("aDouble", "${aDouble}")
                                           .add("anInt", numericValue)
                                           .add("aFloat", "${aFloat}")
                                           .add("aLong", numericValue)
                                           .add("aShort", numericValue)
                                           .build());

          // first test with quotes ("42")
          String jsonPluginStr = GSON.toJson(pluginInfo);
          pluginInfo = GSON.fromJson(jsonPluginStr, Plugin.class);
          instantiator.newInstance(pluginInfo);

          // test without quotes (42)
          pluginInfo = GSON.fromJson(jsonPluginStr.replaceAll("\"" + numericValue + "\"", numericValue), Plugin.class);
          instantiator.newInstance(pluginInfo);

          // test with quotes and dot ("42.0")
          pluginInfo = GSON.fromJson(jsonPluginStr.replaceAll(numericValue, numericValue + ".0"), Plugin.class);
          instantiator.newInstance(pluginInfo);

          // test with dot (42.0)
          pluginInfo = GSON.fromJson(jsonPluginStr.replaceAll("\"" + numericValue + "\"", numericValue + ".0"),
                                     Plugin.class);
          instantiator.newInstance(pluginInfo);

          // test with some actual double number 42.5
          pluginInfo
            = GSON.fromJson(jsonPluginStr.replaceAll("\"" + numericValue + "\"", numericValue + ".5"), Plugin.class);
          try {
            instantiator.newInstance(pluginInfo);
            Assert.fail("Plugin instantiation should fail with value '42.5'");
          } catch (InvalidPluginConfigException e) {
            // expected
          }
        }
      }
    }
  }

  @Test
  public void testPluginProperties() {
    PluginProperties pluginProperties = PluginProperties.builder().add("class.name", TEST_EMPTY_CLASS)
      .add("timeout", "10")
      .add("name", "${macro}")
      .build();
    Assert.assertTrue(pluginProperties.getMacros().getLookups().isEmpty());
    Set<String> lookups = new HashSet<>();
    lookups.add("macro");

    PluginProperties updatedPluginProperties = pluginProperties.setMacros(new Macros(lookups, new HashSet<>()));
    Assert.assertTrue(pluginProperties.getMacros().getLookups().isEmpty());
    Assert.assertEquals(lookups, updatedPluginProperties.getMacros().getLookups());
    Assert.assertTrue(updatedPluginProperties.getMacros().getMacroFunctions().isEmpty());
  }

  @Test
  public void testMacroPlugin() throws Exception {
    File pluginDir = TMP_FOLDER.newFolder();
    addPluginArtifact();
    SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins = getPlugins();
    copyArtifacts(pluginDir, plugins);

    // set up test macro evaluator's substitutions
    Map<String, String> propertySubstitutions = ImmutableMap.<String, String>builder()
      .put("expansiveHostname", "${hostname}/${path}:${port}")
      .put("hostname", "${one}")
      .put("path", "${two}")
      .put("port", "${three}")
      .put("one", "${host${hostScopeMacro}}")
      .put("hostScopeMacro", "-local")
      .put("host-local", "${l}${o}${c}${a}${l}${hostSuffix}")
      .put("l", "l")
      .put("o", "o")
      .put("c", "c")
      .put("a", "a")
      .put("hostSuffix", "host")
      .put("two", "${filename${fileTypeMacro}}")
      .put("three", "${firstPortDigit}${secondPortDigit}")
      .put("filename", "index")
      .put("fileTypeMacro", "-html")
      .put("filename-html", "index.html")
      .put("filename-php", "index.php")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .put("aBoolean", "true")
      .put("aByte", "101")
      .put("aChar", "k")
      .put("aDouble", "64.0")
      .put("aFloat", "52.0")
      .put("anInt", "42")
      .put("aLong", "32")
      .put("aShort", "81")
      .put("authInfo", new Gson().toJson(new TestPlugin.AuthInfo("token", "id")))
      .build();

    // Instantiate the plugins and execute them
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {

      for (Map.Entry<ArtifactDescriptor, Set<PluginClass>> entry : plugins.entrySet()) {
        for (PluginClass pluginClass : entry.getValue()) {
          Plugin pluginInfo = new Plugin(new ArrayList<>(), entry.getKey().getArtifactId(), pluginClass,
                                         PluginProperties.builder().add("class.name", TEST_EMPTY_CLASS)
                                           .add("nullableLongFlag", "10")
                                           .add("host", "${expansiveHostname}")
                                           .add("aBoolean", "${aBoolean}")
                                           .add("aByte", "${aByte}")
                                           .add("aChar", "${aChar}")
                                           .add("aDouble", "${aDouble}")
                                           .add("anInt", "${anInt}")
                                           .add("aFloat", "${aFloat}")
                                           .add("aLong", "${aLong}")
                                           .add("aShort", "${aShort}")
                                           .add("authInfo", "${authInfo}")
                                           .build());

          TestMacroEvaluator testMacroEvaluator = new TestMacroEvaluator(propertySubstitutions, new HashMap<>());
          Callable<String> plugin = instantiator.newInstance(pluginInfo, testMacroEvaluator);
          Assert.assertEquals("localhost/index.html:80,true,101,k,64.0,52.0,42,32,81,AuthInfo{token='token', id='id'}",
                              plugin.call());

          String pluginId = "5";
          PluginContext pluginContext = new DefaultPluginContext(instantiator,
                                                                 NamespaceId.DEFAULT.app("abc").worker("w"),
                                                                 ImmutableMap.of(pluginId, pluginInfo));
          PluginProperties resolvedProperties = pluginContext.getPluginProperties(pluginId, testMacroEvaluator);
          Map<String, String> expected = new HashMap<>();
          expected.put("class.name", TEST_EMPTY_CLASS);
          expected.put("nullableLongFlag", "10");
          expected.put("host", "localhost/index.html:80");
          expected.put("aBoolean", "true");
          expected.put("aByte", "101");
          expected.put("aChar", "k");
          expected.put("aDouble", "64.0");
          expected.put("anInt", "42");
          expected.put("aFloat", "52.0");
          expected.put("aLong", "32");
          expected.put("aShort", "81");
          expected.put("authInfo", propertySubstitutions.get("authInfo"));
          Assert.assertEquals(expected, resolvedProperties.getProperties());
        }
      }
    }
  }

  @Test
  public void testPluginSelector() throws Exception {
    // No plugin yet
    ArtifactRange appArtifactRange = new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(),
                                                       APP_ARTIFACT_ID.getName(),
                                                       APP_ARTIFACT_ID.getVersion(), true,
                                                       APP_ARTIFACT_ID.getVersion(), true);
    try {
      artifactRepository.findPlugin(NamespaceId.DEFAULT, appArtifactRange,
                                    "plugin", "TestPlugin2", new PluginSelector());
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
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(artifact1Id, jarFile, parents, null);

    // Should get the only version.
    Map.Entry<ArtifactDescriptor, PluginClass> plugin =
      artifactRepository.findPlugin(NamespaceId.DEFAULT, appArtifactRange,
                                    "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("1.0"), plugin.getKey().getArtifactId().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());
    Locations.linkOrCopyOverwrite(plugin.getKey().getLocation(),
                                  new File(pluginDir, Artifacts.getFileName(plugin.getKey().getArtifactId())));

    // Create another plugin jar with later version and update the repository
    Id.Artifact artifact2Id = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "2.0");
    jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-2.0.jar"), manifest);
    artifactRepository.addArtifact(artifact2Id, jarFile, parents, null);

    // Should select the latest version
    plugin = artifactRepository.findPlugin(NamespaceId.DEFAULT, appArtifactRange,
                                           "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new ArtifactVersion("2.0"), plugin.getKey().getArtifactId().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());
    Locations.linkOrCopyOverwrite(plugin.getKey().getLocation(),
                                  new File(pluginDir, Artifacts.getFileName(plugin.getKey().getArtifactId())));

    // Load the Plugin class from the classLoader.
    try (PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginDir)) {
      ClassLoader pluginClassLoader = instantiator.getArtifactClassLoader(plugin.getKey().getArtifactId());
      Class<?> pluginClass = pluginClassLoader.loadClass(TestPlugin2.class.getName());

      // Use a custom plugin selector to select with smallest version
      plugin = artifactRepository.findPlugin(NamespaceId.DEFAULT, appArtifactRange,
                                             "plugin", "TestPlugin2", new PluginSelector() {
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

  @Test
  public void testGreatGrandparentsAreInvalid() throws Exception {
    // create child artifact
    io.cdap.cdap.proto.id.ArtifactId childId = NamespaceId.DEFAULT.artifact("child", "1.0.0");
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin1.class.getPackage().getName());
    File jarFile = createPluginJar(Plugin1.class, new File(tmpDir, "child-1.0.0.jar"), manifest);

    // add the artifact
    Set<ArtifactRange> parents = ImmutableSet.of(new ArtifactRange(
      APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(childId), jarFile, parents, null);

    // create grandchild
    io.cdap.cdap.proto.id.ArtifactId grandchildId = NamespaceId.DEFAULT.artifact("grandchild", "1.0.0");
    manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin2.class.getPackage().getName());
    jarFile = createPluginJar(Plugin2.class, new File(tmpDir, "grandchild-1.0.0.jar"), manifest);
    parents = ImmutableSet.of(new ArtifactRange(
      childId.getNamespace(), childId.getArtifact(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(grandchildId), jarFile, parents, null);

    // try and create great grandchild, should fail
    io.cdap.cdap.proto.id.ArtifactId greatGrandchildId = NamespaceId.DEFAULT.artifact("greatgrandchild", "1.0.0");
    manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin2.class.getPackage().getName());
    jarFile = createPluginJar(Plugin2.class, new File(tmpDir, "greatgrandchild-1.0.0.jar"), manifest);
    parents = ImmutableSet.of(new ArtifactRange(
      grandchildId.getNamespace(), grandchildId.getArtifact(),
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    try {
      artifactRepository.addArtifact(Id.Artifact.fromEntityId(greatGrandchildId), jarFile, parents, null);
      Assert.fail("Artifact repository is not supposed to allow great grandparents.");
    } catch (InvalidArtifactException e) {
      // expected
    }
  }

  @Test
  public void testCyclicDependenciesAreInvalid() throws Exception {
    // create a1 artifact that extends app and a2
    io.cdap.cdap.proto.id.ArtifactId a1Id = NamespaceId.DEFAULT.artifact("a1", "1.0.0");
    io.cdap.cdap.proto.id.ArtifactId a2Id = NamespaceId.DEFAULT.artifact("a2", "1.0.0");
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin1.class.getPackage().getName());
    File jarFile = createPluginJar(Plugin1.class, new File(tmpDir, "a1-1.0.0.jar"), manifest);
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")),
      new ArtifactRange(a2Id.getNamespace(), a2Id.getArtifact(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(Id.Artifact.fromEntityId(a1Id), jarFile, parents, null);

    // create a2 artifact that extends a1, which should be invalid
    manifest = createManifest(ManifestFields.EXPORT_PACKAGE, Plugin2.class.getPackage().getName());
    jarFile = createPluginJar(Plugin2.class, new File(tmpDir, "a2-1.0.0.jar"), manifest);
    parents = ImmutableSet.of(
      new ArtifactRange(a1Id.getNamespace(), a1Id.getArtifact(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    try {
      artifactRepository.addArtifact(Id.Artifact.fromEntityId(a2Id), jarFile, parents, null);
      Assert.fail("Artifact repository did not catch a cyclic dependency.");
    } catch (InvalidArtifactException e) {
      // expected
    }
  }

  @Test
  public void testPluginMetadata() throws Exception {
    // Create a plugin jar. It contains two plugins, TestPlugin and TestPlugin2 inside.
    Id.Artifact artifact1Id = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "1.0");
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    artifactRepository.addArtifact(artifact1Id, jarFile, parents, null);

    PluginId testPlugin1 = new PluginId("default", "myPlugin", "1.0", "TestPlugin", "plugin");
    Metadata expected = new Metadata(MetadataScope.SYSTEM, ImmutableSet.of("tag1", "tag2", "tag3"),
                                     ImmutableMap.of("k1", "v1", "k2", "v2"));
    Assert.assertEquals(expected, metadataAdmin.getMetadata(testPlugin1.toMetadataEntity()));

    PluginId testPlugin2 = new PluginId("default", "myPlugin", "1.0", "TestPlugin2", "plugin");
    expected = new Metadata(MetadataScope.SYSTEM, ImmutableSet.of("test-tag1", "test-tag2", "test-tag3"),
                            ImmutableMap.of("key1", "val1", "key2", "val2"));
    Assert.assertEquals(expected, metadataAdmin.getMetadata(testPlugin2.toMetadataEntity()));

    // test metadata is cleaned up when the artifact gets deleted
    artifactRepository.deleteArtifact(artifact1Id);

    Assert.assertEquals(Metadata.EMPTY, metadataAdmin.getMetadata(testPlugin1.toMetadataEntity()));
    Assert.assertEquals(Metadata.EMPTY, metadataAdmin.getMetadata(testPlugin2.toMetadataEntity()));
  }

  @Test
  public void testArtifactProperties() throws Exception {
    // test adding properties
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    artifactRepository.writeArtifactProperties(APP_ARTIFACT_ID, properties);
    ArtifactDetail detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(properties, detail.getMeta().getProperties());

    // test properties are overwritten
    properties = ImmutableMap.of("k2", "v2-2", "k3", "v3");
    artifactRepository.writeArtifactProperties(APP_ARTIFACT_ID, properties);
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(properties, detail.getMeta().getProperties());

    // test deleting a property
    artifactRepository.deleteArtifactProperty(APP_ARTIFACT_ID, "k2");
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(ImmutableMap.of("k3", "v3"), detail.getMeta().getProperties());

    // test deleting a non-existant property is a no-op
    artifactRepository.deleteArtifactProperty(APP_ARTIFACT_ID, "k2");
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(ImmutableMap.of("k3", "v3"), detail.getMeta().getProperties());

    // test updating one property
    artifactRepository.writeArtifactProperty(APP_ARTIFACT_ID, "k3", "v3-2");
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(ImmutableMap.of("k3", "v3-2"), detail.getMeta().getProperties());

    // test writing one new property
    artifactRepository.writeArtifactProperty(APP_ARTIFACT_ID, "k2", "v2-3");
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(ImmutableMap.of("k2", "v2-3", "k3", "v3-2"), detail.getMeta().getProperties());

    // test deleting all properties
    artifactRepository.deleteArtifactProperties(APP_ARTIFACT_ID);
    detail = artifactRepository.getArtifact(APP_ARTIFACT_ID);
    Assert.assertEquals(0, detail.getMeta().getProperties().size());
  }

  @Test
  public void testNamespaceIsolation() throws Exception {
    // create system app artifact
    Id.Artifact systemAppArtifactId = Id.Artifact.from(Id.Namespace.SYSTEM, "PluginTest", "1.0.0");
    File jar = createAppJar(PluginTestApp.class, new File(systemArtifactsDir1, "PluginTest-1.0.0.jar"),
                            createManifest(ManifestFields.EXPORT_PACKAGE,
                                           PluginTestRunnable.class.getPackage().getName()));
    artifactRepository.addSystemArtifacts();
    Assert.assertTrue(jar.delete());

    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(systemAppArtifactId.getNamespace().getId(), systemAppArtifactId.getName(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    NamespaceId namespace1 = Ids.namespace("ns1");
    NamespaceId namespace2 = Ids.namespace("ns2");

    Id.Artifact pluginArtifactId1 = Id.Artifact.from(Id.Namespace.fromEntityId(namespace1), "myPlugin", "1.0");
    Id.Artifact pluginArtifactId2 = Id.Artifact.from(Id.Namespace.fromEntityId(namespace2), "myPlugin", "1.0");

    try {
      // create plugin artifact in namespace1 that extends the system artifact
      // There should be two plugins there (TestPlugin and TestPlugin2).
      Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
      File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);
      artifactRepository.addArtifact(pluginArtifactId1, jarFile, parents, null);

      // create plugin artifact in namespace2 that extends the system artifact
      artifactRepository.addArtifact(pluginArtifactId2, jarFile, parents, null);

      // check that only plugins from the artifact in the namespace are returned, and not plugins from both
      SortedMap<ArtifactDescriptor, Set<PluginClass>> extensions =
        artifactRepository.getPlugins(namespace1, systemAppArtifactId);
      Assert.assertEquals(1, extensions.keySet().size());
      Assert.assertEquals(2, extensions.values().iterator().next().size());

      extensions = artifactRepository.getPlugins(namespace2, systemAppArtifactId);
      Assert.assertEquals(1, extensions.keySet().size());
      Assert.assertEquals(2, extensions.values().iterator().next().size());
    } finally {
      artifactRepository.clear(NamespaceId.SYSTEM);
      artifactRepository.clear(namespace1);
      artifactRepository.clear(namespace2);
    }
  }

  private static void addPluginArtifact() throws Exception {
    addPluginArtifact(Collections.singleton(
      new ArtifactRange(APP_ARTIFACT_ID.getNamespace().getId(), APP_ARTIFACT_ID.getName(),
                        new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))));
  }

  private static void addPluginArtifact(Set<ArtifactRange> parents) throws Exception {
    // Create the plugin jar. There should be two plugins there (TestPlugin and TestPlugin2).
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    File jarFile = createPluginJar(TestPlugin.class, new File(tmpDir, "myPlugin-1.0.jar"), manifest);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "myPlugin", "1.0");
    artifactRepository.addArtifact(artifactId, jarFile, parents, null);
  }

  private static SortedMap<ArtifactDescriptor, Set<PluginClass>> getPlugins() throws Exception {
    // check the parent can see the plugins
    SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins =
      artifactRepository.getPlugins(NamespaceId.DEFAULT, APP_ARTIFACT_ID);
    Assert.assertEquals(1, plugins.size());
    Assert.assertEquals(2, plugins.get(plugins.firstKey()).size());
    return plugins;
  }

  private static void copyArtifacts(File pluginDir, SortedMap<ArtifactDescriptor, Set<PluginClass>> plugins)
    throws IOException {
    ArtifactDescriptor descriptor = plugins.firstKey();
    Locations.linkOrCopyOverwrite(descriptor.getLocation(),
                                  new File(pluginDir, Artifacts.getFileName(descriptor.getArtifactId())));
  }

  private static ProgramClassLoader createAppClassLoader(File jarFile) throws IOException {
    File unpackDir = DirUtils.createTempDir(TMP_FOLDER.newFolder());
    BundleJarUtil.prepareClassLoaderFolder(jarFile, unpackDir);
    return new ProgramClassLoader(cConf, unpackDir,
                                  FilterClassLoader.create(ArtifactRepositoryTest.class.getClassLoader()));
  }

  private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
    return destFile;
  }

  private static File createPluginJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = PluginJarHelper.createPluginJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                             manifest, cls);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
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
