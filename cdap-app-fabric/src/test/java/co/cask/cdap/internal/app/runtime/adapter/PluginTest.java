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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.PluginTestAppTemplate;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.api.templates.plugins.PluginVersion;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.plugins.test.TestPlugin;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Unit-tests for app template plugin support.
 */
public class PluginTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final String TEMPLATE_NAME = "PluginTest";

  private static CConfiguration cConf;
  private static File appTemplateJar;
  private static ApplicationTemplateInfo appTemplateInfo;
  private static File templatePluginDir;
  private static ClassLoader templateClassLoader;

  @BeforeClass
  public static void setup() throws IOException, ClassNotFoundException {
    LoggerFactory.getLogger(PluginTest.class).info("Testing {}", cConf);

    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    DirUtils.mkdirs(new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR)));

    // Create the template jar
    File appTemplateDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    appTemplateJar = createJar(PluginTestAppTemplate.class, new File(appTemplateDir, "PluginTest-1.0.jar"));
    appTemplateInfo = new ApplicationTemplateInfo(appTemplateJar, TEMPLATE_NAME, TEMPLATE_NAME,
                                                  ProgramType.WORKER, Files.hash(appTemplateJar, Hashing.md5()));

    templateClassLoader = createAppClassLoader(appTemplateJar);

    // Create the plugin directory for the PluginTest template
    templatePluginDir = new File(appTemplateDir, TEMPLATE_NAME);
    DirUtils.mkdirs(templatePluginDir);

    // Create a lib jar that is shared among all plugins for the template.
    File libDir = TMP_FOLDER.newFolder();
    generateClass(EmptyClass.class, "test.EmptyClass", libDir);
    createJar(new DirectoryClassLoader(libDir, null).loadClass("test.EmptyClass"),
              new File(new File(templatePluginDir, "lib"), "common.jar"));
  }

  @After
  public void cleanupPlugins() throws IOException {
    // Cleanup the jars in the plugin directory for the template,
    // as each test creates plugin jar for different kind of testing
    for (File jarFile : DirUtils.listFiles(templatePluginDir, "jar")) {
      jarFile.delete();
    }
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
    createJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    PluginRepository repository = new PluginRepository(cConf);
    Multimap<PluginInfo, PluginClass> pluginInfos = repository.inspectPlugins(TEMPLATE_NAME, appTemplateJar);
    Assert.assertEquals(2, pluginInfos.size());

    // Instantiate the plugins and execute them
    PluginInstantiator instantiator = new PluginInstantiator(cConf, TEMPLATE_NAME, templateClassLoader);
    for (Map.Entry<PluginInfo, PluginClass> entry : pluginInfos.entries()) {
      Callable<String> plugin = instantiator.newInstance(entry.getKey(), entry.getValue(),
                                                         PluginProperties.builder()
                                                          .add("class.name", "test.EmptyClass")
                                                          .add("timeout", "10")
                                                          .build()
      );

      Assert.assertEquals("test.EmptyClass", plugin.call());
    }
  }

  @Test
  public void testExternalConfig() throws IOException {
    // For testing plugins that are configure externally through a json file.
    // Create a jar, without any export package information
    createJar(TestPlugin.class, new File(templatePluginDir, "external-plugin-1.0.jar"));

    // Create a config json file.
    PluginClass pluginClass = new PluginClass("plugin", "External", "External Plugin", TestPlugin.class.getName(), null,
      ImmutableMap.of(
        "class.name", new PluginPropertyField("class.name", "Name of the class", "string", true),
        "timeout", new PluginPropertyField("timeout", "Timeout value", "long", false)
      ));
    File configFile = new File(templatePluginDir, "external-plugin-1.0.json");
    Writer writer = Files.newWriter(configFile, Charsets.UTF_8);
    try {
      new Gson().toJson(ImmutableList.of(pluginClass), writer);
    } finally {
      writer.close();
    }

    // Build up the plugin repository.
    PluginRepository repository = new PluginRepository(cConf);
    Multimap<PluginInfo, PluginClass> plugins = repository.inspectPlugins(TEMPLATE_NAME, appTemplateJar);

    // There should be one for the external-plugin
    Map.Entry<PluginInfo, PluginClass> pluginEntry = null;
    for (Map.Entry<PluginInfo, PluginClass> entry : plugins.entries()) {
      if (entry.getKey().getName().equals("external-plugin")) {
        pluginEntry = entry;
        break;
      }
    }

    Assert.assertNotNull(pluginEntry);
    // There should be exactly one plugin class for the external plugin.
    Assert.assertEquals(1, plugins.get(pluginEntry.getKey()).size());
    Assert.assertEquals(pluginClass, pluginEntry.getValue());
  }

  @Test
  public void testPluginSelector() throws IOException {
    PluginRepository repository = new PluginRepository(cConf);

    // No plugin yet
    Map.Entry<PluginInfo, PluginClass> plugin = repository.findPlugin(TEMPLATE_NAME,
                                                                      "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNull(plugin);

    // Create a plugin jar
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    createJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    repository.inspectPlugins(ImmutableList.of(appTemplateInfo));

    // Should get the only version.
    plugin = repository.findPlugin(TEMPLATE_NAME, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new PluginVersion("1.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Create another plugin jar with later version and update the repository
    createJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-2.0.jar"), manifest);
    repository.inspectPlugins(ImmutableList.of(appTemplateInfo));

    // Should select the latest version
    plugin = repository.findPlugin(TEMPLATE_NAME, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new PluginVersion("2.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Use a custom plugin selector to select with smallest version
    plugin = repository.findPlugin(TEMPLATE_NAME, "plugin", "TestPlugin2", new PluginSelector() {
      @Override
      public Map.Entry<PluginInfo, PluginClass> select(SortedMap<PluginInfo, PluginClass> plugins) {
        return plugins.entrySet().iterator().next();
      }
    });
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new PluginVersion("1.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());
  }

  private static ClassLoader createAppClassLoader(File jarFile) throws IOException {
    final File unpackDir = DirUtils.createTempDir(TMP_FOLDER.newFolder());
    BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(jarFile), unpackDir);
    return ProgramClassLoader.create(unpackDir, PluginTest.class.getClassLoader());
  }

  private static File createJar(Class<?> cls, File destFile) throws IOException {
    return createJar(cls, destFile, new Manifest());
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }

  private Manifest createManifest(Object...entries) {
    Preconditions.checkArgument(entries.length % 2 == 0);
    Attributes attributes = new Attributes();
    for (int i = 0; i < entries.length; i += 2) {
      attributes.put(entries[i], entries[i + 1]);
    }
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);
    return manifest;
  }

  private static File generateClass(Class<?> fromClass, final String className, File directory) throws IOException {
    // Generate a class dynamically using ASM from another class, but use a different class name,
    // so that it won't be in the test class path.
    InputStream byteCode = fromClass.getClassLoader().getResourceAsStream(Type.getInternalName(fromClass) + ".class");
    try {
      ClassReader reader = new ClassReader(byteCode);
      ClassWriter writer = new ClassWriter(0);
      reader.accept(new ClassVisitor(Opcodes.ASM4, writer) {
        @Override
        public void visit(int version, int access, String name, String signature,
                          String superName, String[] interfaces) {
          super.visit(version, access, className.replace('.', '/'), signature, superName, interfaces);
        }
      }, 0);

      File target = new File(directory, className.replace('.', File.separatorChar) + ".class");
      DirUtils.mkdirs(target.getParentFile());
      Files.write(writer.toByteArray(), target);
      return target;
    } finally {
      byteCode.close();
    }
  }
}
