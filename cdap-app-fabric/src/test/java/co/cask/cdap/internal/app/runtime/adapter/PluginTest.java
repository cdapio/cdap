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

import co.cask.cdap.api.app.Application;
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
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.plugins.template.test.PluginTestAppTemplate;
import co.cask.cdap.internal.app.plugins.template.test.api.PluginTestRunnable;
import co.cask.cdap.internal.app.plugins.test.TestPlugin;
import co.cask.cdap.internal.app.plugins.test.TestPlugin2;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Unit-tests for app template plugin support.
 */
public class PluginTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final String TEMPLATE_NAME = "PluginTest";
  private static final Gson GSON = new Gson();
  private static final String TEST_EMPTY_CLASS = "test.EmptyClass";

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
    cConf.set(Constants.AppFabric.APP_TEMPLATE_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    // Create the template jar, with the package of PluginTestRunnable exported
    File appTemplateDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    DirUtils.mkdirs(appTemplateDir);
    appTemplateJar = createJar(PluginTestAppTemplate.class, new File(appTemplateDir, "PluginTest-1.0.jar"),
                               createManifest(ManifestFields.EXPORT_PACKAGE,
                                              PluginTestRunnable.class.getPackage().getName()));
    appTemplateInfo = new ApplicationTemplateInfo(appTemplateJar, TEMPLATE_NAME, TEMPLATE_NAME,
                                                  ProgramType.WORKER, Files.hash(appTemplateJar, Hashing.md5()));

    templateClassLoader = createAppClassLoader(appTemplateJar);

    // Create the plugin directory for the PluginTest template
    templatePluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR), TEMPLATE_NAME);
    DirUtils.mkdirs(templatePluginDir);

    // Create a lib jar that is shared among all plugins for the template.
    File libDir = TMP_FOLDER.newFolder();
    generateClass(EmptyClass.class, TEST_EMPTY_CLASS, libDir);
    createJar(libDir, new File(new File(templatePluginDir, "lib"), "common.jar"));
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
  public void testPlugin() throws Exception {
    // Create the plugin jar. There should be two plugins there (TestPlugin and TestPlugin2).
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    createPluginJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    PluginRepository repository = new PluginRepository(cConf);
    Multimap<PluginInfo, PluginClass> pluginInfos = repository.inspectPlugins(TEMPLATE_NAME, appTemplateJar);
    Assert.assertEquals(2, pluginInfos.size());

    // Instantiate the plugins and execute them
    PluginInstantiator instantiator = new PluginInstantiator(cConf, TEMPLATE_NAME, templateClassLoader);
    for (Map.Entry<PluginInfo, PluginClass> entry : pluginInfos.entries()) {
      Callable<String> plugin = instantiator.newInstance(entry.getKey(), entry.getValue(),
                                                         PluginProperties.builder()
                                                          .add("class.name", TEST_EMPTY_CLASS)
                                                          .add("timeout", "10")
                                                          .build()
      );

      Assert.assertEquals(TEST_EMPTY_CLASS, plugin.call());
    }
  }

  @Test
  public void testExternalConfig() throws IOException, URISyntaxException {
    // For testing plugins that are configure externally through a json file.
    // Create a jar, without any export package information
    createPluginJar(TestPlugin.class, new File(templatePluginDir, "external-plugin-1.0.jar"), new Manifest());
    URL externalJar = getClass().getClassLoader().getResource("invalid_plugin.jar");
    if (externalJar != null) {
      Files.copy(new File(externalJar.toURI()), new File(templatePluginDir, "invalid-plugin-1.0.jar"));
    }

    // Create a config json file that expose two plugins (to the same class).
    // One of the plugin has no property field
    List<JsonObject> pluginDefs = ImmutableList.of(
      createPluginJson("plugin", "External", "External Plugin", TestPlugin.class.getName(),
                       new PluginPropertyField("class.name", "Name of the class", "string", true),
                       new PluginPropertyField("timeout", "Timeout value", "long", false)
      ),
      createPluginJson("plugin2", "External2", "External Plugin2", TestPlugin.class.getName())
    );

    File configFile = new File(templatePluginDir, "external-plugin-1.0.json");
    try (Writer writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      GSON.toJson(pluginDefs, writer);
    }

    // Build up the plugin repository.
    PluginRepository repository = new PluginRepository(cConf);
    TreeMultimap<PluginInfo, PluginClass> plugins = repository.inspectPlugins(TEMPLATE_NAME, appTemplateJar);

    // There should be one for the external-plugin
    PluginInfo pluginInfo = null;
    for (Map.Entry<PluginInfo, PluginClass> entry : plugins.entries()) {
      if (entry.getKey().getName().equals("external-plugin")) {
        pluginInfo = entry.getKey();
        break;
      }
    }

    Assert.assertNotNull(pluginInfo);

    // There should be two plugin classes
    Assert.assertEquals(2, plugins.get(pluginInfo).size());

    // The first one have two property fields, the second one has no property field
    // The collection is always sorted by the plugin name (guaranteed by plugin repository
    PluginClass pluginClass = plugins.get(pluginInfo).first();
    Assert.assertEquals("External", pluginClass.getName());
    Assert.assertEquals(2, pluginClass.getProperties().size());

    pluginClass = plugins.get(pluginInfo).last();
    Assert.assertEquals("External2", pluginClass.getName());
    Assert.assertEquals(0, pluginClass.getProperties().size());
  }

  @Test
  public void testPluginSelector() throws IOException, ClassNotFoundException {
    PluginRepository repository = new PluginRepository(cConf);

    // No plugin yet
    Map.Entry<PluginInfo, PluginClass> plugin = repository.findPlugin(TEMPLATE_NAME,
                                                                      "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNull(plugin);

    // Create a plugin jar. It contains two plugins, TestPlugin and TestPlugin2 inside.
    Manifest manifest = createManifest(ManifestFields.EXPORT_PACKAGE, TestPlugin.class.getPackage().getName());
    createPluginJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-1.0.jar"), manifest);

    // Build up the plugin repository.
    repository.inspectPlugins(ImmutableList.of(appTemplateInfo));

    // Should get the only version.
    plugin = repository.findPlugin(TEMPLATE_NAME, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new PluginVersion("1.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Create another plugin jar with later version and update the repository
    createPluginJar(TestPlugin.class, new File(templatePluginDir, "myPlugin-2.0.jar"), manifest);
    repository.inspectPlugins(ImmutableList.of(appTemplateInfo));

    // Should select the latest version
    plugin = repository.findPlugin(TEMPLATE_NAME, "plugin", "TestPlugin2", new PluginSelector());
    Assert.assertNotNull(plugin);
    Assert.assertEquals(new PluginVersion("2.0"), plugin.getKey().getVersion());
    Assert.assertEquals("TestPlugin2", plugin.getValue().getName());

    // Load the Plugin class and the common "EmptyClass" from the classLoader.
    PluginInstantiator instantiator = new PluginInstantiator(cConf, TEMPLATE_NAME, templateClassLoader);
    ClassLoader pluginClassLoader = instantiator.getPluginClassLoader(plugin.getKey());
    Class<?> pluginClass = pluginClassLoader.loadClass(TestPlugin2.class.getName());
    Class<?> emptyClass = pluginClassLoader.loadClass(TEST_EMPTY_CLASS);

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

    // Load the Plugin class and the "EmptyClass" again from the current plugin selected
    // The plugin class should be different (from different ClassLoader)
    // The empty class should be the same (from the plugin lib ClassLoader)
    pluginClassLoader = instantiator.getPluginClassLoader(plugin.getKey());
    Assert.assertNotSame(pluginClass, pluginClassLoader.loadClass(TestPlugin2.class.getName()));
    Assert.assertSame(emptyClass, pluginClassLoader.loadClass(TEST_EMPTY_CLASS));

    // From the pluginClassLoader, loading export classes from the template jar should be allowed
    Class<?> cls = pluginClassLoader.loadClass(PluginTestRunnable.class.getName());
    // The class should be loaded from the template classloader
    Assert.assertSame(templateClassLoader.loadClass(PluginTestRunnable.class.getName()), cls);

    // From the plugin classloader, all cdap api classes is loadable as well.
    cls = pluginClassLoader.loadClass(Application.class.getName());
    // The Application class should be the same as the one in the system classloader
    Assert.assertSame(Application.class, cls);
  }

  private static ClassLoader createAppClassLoader(File jarFile) throws IOException {
    final File unpackDir = DirUtils.createTempDir(TMP_FOLDER.newFolder());
    BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(jarFile), unpackDir);
    return ProgramClassLoader.create(unpackDir, PluginTest.class.getClassLoader());
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
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

  private static File createJar(File sourceDir, File jarFile) throws IOException {
    DirUtils.mkdirs(jarFile.getParentFile());

    URI relativeURI = sourceDir.toURI();
    try (JarOutputStream output = new JarOutputStream(new FileOutputStream(jarFile))) {
      Queue<File> queue = Lists.newLinkedList();
      queue.add(sourceDir);
      while (!queue.isEmpty()) {
        File file = queue.poll();
        String name = relativeURI.relativize(file.toURI()).getPath();
        if (!name.isEmpty()) {
          output.putNextEntry(new JarEntry(name));
        }

        if (file.isDirectory()) {
          queue.addAll(DirUtils.listFiles(file));
        } else {
          Files.copy(file, output);
        }
      }
    }

    return jarFile;
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

  private static File generateClass(Class<?> fromClass, final String className, File directory) throws IOException {
    // Generate a class dynamically using ASM from another class, but use a different class name,
    // so that it won't be in the test class path.
    try (
      InputStream byteCode = fromClass.getClassLoader().getResourceAsStream(Type.getInternalName(fromClass) + ".class")
    ) {
      ClassReader reader = new ClassReader(byteCode);
      ClassWriter writer = new ClassWriter(0);
      reader.accept(new ClassVisitor(Opcodes.ASM5, writer) {
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
    }
  }

  private JsonObject createPluginJson(String type, String name, String description,
                                      String className, PluginPropertyField...fields) {
    JsonObject json = new JsonObject();
    json.addProperty("type", type);
    json.addProperty("name", name);
    json.addProperty("description", description);
    json.addProperty("className", className);
    if (fields.length > 0) {
      Map<String, PluginPropertyField> properties = Maps.newHashMap();
      for (PluginPropertyField field : fields) {
        properties.put(field.getName(), field);
      }
      json.add("properties", GSON.toJsonTree(properties));
    }
    return json;
  }
}
