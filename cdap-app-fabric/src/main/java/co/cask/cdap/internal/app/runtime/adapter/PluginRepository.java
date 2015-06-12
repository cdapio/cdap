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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.api.templates.plugins.PluginVersion;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.Files;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import javax.annotation.Nullable;

/**
 * This class manage plugin information that are available for application templates.
 */
public class PluginRepository {

  private static final Logger LOG = LoggerFactory.getLogger(PluginRepository.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();

  // Object type for data in the config json file.
  private static final Type CONFIG_OBJECT_TYPE = new TypeToken<List<PluginClass>>() { }.getType();

  // Transform File into PluginInfo, assuming the plugin file name is in form [name][separator][version].jar
  private static final Function<File, PluginFile> FILE_TO_PLUGIN_FILE = new Function<File, PluginFile>() {
    @Override
    public PluginFile apply(File file) {
      String plugin = file.getName().substring(0, file.getName().length() - ".jar".length());
      PluginVersion version = new PluginVersion(plugin, true);
      String rawVersion = version.getVersion();

      String pluginName = rawVersion == null ? plugin : plugin.substring(0, plugin.length() - rawVersion.length() - 1);
      return new PluginFile(file, new PluginInfo(file.getName(), pluginName, version));
    }
  };

  private final CConfiguration cConf;
  private final File pluginDir;
  private final File tmpDir;
  private final AtomicReference<Map<String, TreeMultimap<PluginInfo, PluginClass>>> plugins;

  @Inject
  PluginRepository(CConfiguration cConf) {
    this.cConf = cConf;
    this.pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));
    this.plugins = new AtomicReference<Map<String, TreeMultimap<PluginInfo, PluginClass>>>(
      new HashMap<String, TreeMultimap<PluginInfo, PluginClass>>());
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Returns a {@link SortedMap} of all plugins available for the given application template. The keys
   * are sorted by the {@link PluginInfo}. Only unique {@link PluginClass} are returned in the value
   * Collection, where uniqueness is determined by the plugin class type and name only.
   *
   * @param template name of the template
   */
  public SortedMap<PluginInfo, Collection<PluginClass>> getPlugins(String template) {
    TreeMultimap<PluginInfo, PluginClass> result = plugins.get().get(template);
    return result == null ? ImmutableSortedMap.<PluginInfo, Collection<PluginClass>>of()
                          : Collections.unmodifiableSortedMap(result.asMap());
  }

  /**
   * Returns a {@link Map.Entry} represents the plugin information for the plugin being requested.
   *
   * @param template name of the template
   * @param pluginType plugin type name
   * @param pluginName plugin name
   * @param selector for selecting which plugin to use
   * @return the entry found or {@code null} if none was found
   */
  @Nullable
  public Map.Entry<PluginInfo, PluginClass> findPlugin(String template, final String pluginType,
                                                       final String pluginName, PluginSelector selector) {
    // Transform by matching type, name. If there is no match, the map value is null.
    // We then filter out null value
    SortedMap<PluginInfo, PluginClass> plugins = ImmutableSortedMap.copyOf(Maps.filterValues(Maps.transformValues(
      getPlugins(template), new Function<Collection<PluginClass>, PluginClass>() {
        @Nullable
        @Override
        public PluginClass apply(Collection<PluginClass> input) {
          for (PluginClass pluginClass : input) {
            if (pluginClass.getType().equals(pluginType) && pluginClass.getName().equals(pluginName)) {
              return pluginClass;
            }
          }
          return null;
        }
      }), Predicates.notNull()));

    return plugins.isEmpty() ? null : selector.select(plugins);
  }

  /**
   * Inspects plugins for all the templates.
   *
   * @param templates list of template information
   */
  void inspectPlugins(Iterable<? extends ApplicationTemplateInfo> templates) throws IOException {
    Map<String, TreeMultimap<PluginInfo, PluginClass>> result = Maps.newHashMap();
    for (ApplicationTemplateInfo info : templates) {
      result.put(info.getName(), inspectPlugins(info.getName(), info.getFile()));
    }
    plugins.set(result);
  }

  /**
   * Inspects and builds plugin information for the given application template.
   *
   * @param template name of the template
   * @param templateJar application jar for the application template
   */
  @VisibleForTesting
  TreeMultimap<PluginInfo, PluginClass> inspectPlugins(String template, File templateJar) throws IOException {
    // We want the plugins sorted by the PluginInfo, which in turn is sorted by name and version.
    // Also the PluginClass should be unique by its (type, name).
    TreeMultimap<PluginInfo, PluginClass> templatePlugins = TreeMultimap.create(new PluginInfoComaprator(),
                                                                                new PluginClassComparator());
    File templatePluginDir = new File(pluginDir, template);
    List<File> pluginJars = DirUtils.listFiles(templatePluginDir, "jar");
    if (pluginJars.isEmpty()) {
      return templatePlugins;
    }

    Iterable<PluginFile> pluginFiles = Iterables.transform(pluginJars, FILE_TO_PLUGIN_FILE);
    try (
      CloseableClassLoader templateClassLoader = createTemplateClassLoader(templateJar);
      PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf, template, templateClassLoader)
    ) {
      for (PluginFile pluginFile : pluginFiles) {
        if (!configureByFile(pluginFile, templatePlugins)) {
          configureByInspection(pluginFile, pluginInstantiator, templatePlugins);
        }
      }
    }

    return templatePlugins;
  }

  /**
   * Gathers plugin class information by parsing an external configuration file.
   *
   * @return {@code true} if there is an external configuration file, {@code false} otherwise.
   */
  private boolean configureByFile(PluginFile pluginFile,
                                  Multimap<PluginInfo, PluginClass> templatePlugins) throws IOException {
    String pluginFileName = pluginFile.getFile().getName();
    String configFileName = pluginFileName.substring(0, pluginFileName.length() - ".jar".length()) + ".json";

    File configFile = new File(pluginFile.getFile().getParentFile(), configFileName);
    if (!configFile.isFile()) {
      return false;
    }

    // The config file is a json array of PluginClass object (except the PluginClass.configFieldName)
    try (Reader reader = Files.newReader(configFile, Charsets.UTF_8)) {
      List<PluginClass> pluginClasses = GSON.fromJson(reader, CONFIG_OBJECT_TYPE);

      // Put it one by one so that we can log duplicate plugin class
      for (PluginClass pluginClass : pluginClasses) {
        if (!templatePlugins.put(pluginFile.getPluginInfo(), pluginClass)) {
          LOG.warn("Plugin already exists in {}. Ignore plugin class {}", pluginFile.getPluginInfo(), pluginClass);
        }
      }
      templatePlugins.putAll(pluginFile.getPluginInfo(), pluginClasses);
    }

    return true;
  }

  /**
   * Inspects the plugin file and extracts plugin classes information.
   */
  private void configureByInspection(PluginFile pluginFile, PluginInstantiator pluginInstantiator,
                                     Multimap<PluginInfo, PluginClass> templatePlugins) throws IOException {

    // See if there are export packages. Plugins should be in those packages
    Set<String> exportPackages = getExportPackages(pluginFile.getFile());
    if (exportPackages.isEmpty()) {
      return;
    }

    // Load the plugin class and inspect the config field.
    ClassLoader pluginClassLoader = pluginInstantiator.getPluginClassLoader(pluginFile.getPluginInfo());
    for (Class<?> cls : getPluginClasses(exportPackages, pluginClassLoader)) {
      Plugin pluginAnnotation = cls.getAnnotation(Plugin.class);
      if (pluginAnnotation == null) {
        continue;
      }
      Map<String, PluginPropertyField> pluginProperties = Maps.newHashMap();
      try {
        String configField = getProperties(TypeToken.of(cls), pluginProperties);
        PluginClass pluginClass = new PluginClass(pluginAnnotation.type(), getPluginName(cls),
                                                  getPluginDescription(cls), cls.getName(),
                                                  configField, pluginProperties);

        if (!templatePlugins.put(pluginFile.getPluginInfo(), pluginClass)) {
          LOG.warn("Plugin already exists in {}. Ignore plugin class {}", pluginFile.getPluginInfo(), pluginClass);
        }
      } catch (UnsupportedTypeException e) {
        LOG.warn("Plugin configuration type not supported. Plugin ignored. {}", cls, e);
      }
    }
  }

  /**
   * Returns the set of package names that are declared in "Export-Package" in the jar file Manifest.
   */
  private Set<String> getExportPackages(File file) throws IOException {
    try (JarFile jarFile = new JarFile(file)) {
      return ManifestFields.getExportPackages(jarFile.getManifest());
    }
  }

  /**
   * Returns an {@link Iterable} of class name that are under the given list of package names that are loadable
   * through the plugin ClassLoader.
   */
  private Iterable<Class<?>> getPluginClasses(final Iterable<String> packages, final ClassLoader pluginClassLoader) {
    return new Iterable<Class<?>>() {
      @Override
      public Iterator<Class<?>> iterator() {
        final Iterator<String> packageIterator = packages.iterator();

        return new AbstractIterator<Class<?>>() {
          Iterator<String> classIterator = ImmutableList.<String>of().iterator();
          String currentPackage;

          @Override
          protected Class<?> computeNext() {
            while (!classIterator.hasNext()) {
              if (!packageIterator.hasNext()) {
                return endOfData();
              }
              currentPackage = packageIterator.next();

              try {
                // Gets all package resource URL for the given package
                String resourceName = currentPackage.replace('.', File.separatorChar);
                Enumeration<URL> resources = pluginClassLoader.getResources(resourceName);
                while (resources.hasMoreElements()) {
                  URL packageResource = resources.nextElement();

                  // Only inspect classes in the top level jar file for Plugins.
                  // The jar manifest may have packages in Export-Package that are loadable from the bundled jar files,
                  // which is for classloading purpose. Those classes won't be inspected for plugin classes.
                  // There should be exactly one of resource that match, because it maps to a directory on the FS.
                  if (packageResource.getProtocol().equals("file")) {
                    classIterator = DirUtils.list(new File(packageResource.toURI()), "class").iterator();
                    break;
                  }
                }
              } catch (Exception e) {
                // Cannot happen
                throw Throwables.propagate(e);
              }
            }

            try {
              return pluginClassLoader.loadClass(getClassName(currentPackage, classIterator.next()));
            } catch (ClassNotFoundException e) {
              // Cannot happen, since the class name is from the list of the class files under the classloader.
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }

  /**
   * Creates a ClassLoader for the given template application.
   *
   * @param templateJar the template jar file.
   * @return a {@link CloseableClassLoader} for the template application.
   * @throws IOException if failed to expand the jar
   */
  private CloseableClassLoader createTemplateClassLoader(File templateJar) throws IOException {
    final File unpackDir = DirUtils.createTempDir(tmpDir);
    BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(templateJar), unpackDir);
    ProgramClassLoader programClassLoader = ProgramClassLoader.create(unpackDir, getClass().getClassLoader());
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() {
        try {
          DirUtils.deleteDirectoryContents(unpackDir);
        } catch (IOException e) {
          LOG.warn("Failed to delete directory {}", unpackDir, e);
        }
      }
    });
  }

  /**
   * Extracts and returns name of the plugin.
   */
  private String getPluginName(Class<?> cls) {
    Name annotation = cls.getAnnotation(Name.class);
    return annotation == null || annotation.value().isEmpty() ? cls.getName() : annotation.value();
  }

  /**
   * Returns description for the plugin.
   */
  private String getPluginDescription(Class<?> cls) {
    Description annotation = cls.getAnnotation(Description.class);
    return annotation == null ? "" : annotation.value();
  }

  /**
   * Constructs the fully qualified class name based on the package name and the class file name.
   */
  private String getClassName(String packageName, String classFileName) {
    return packageName + "." + classFileName.substring(0, classFileName.length() - ".class".length());
  }

  /**
   * Gets all config properties for the given plugin.
   *
   * @return the name of the config field in the plugin class or {@code null} if the plugin doesn't have a config field
   */
  @Nullable
  private String getProperties(TypeToken<?> pluginType,
                               Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    // Get the config field
    for (TypeToken<?> type : pluginType.getTypes().classes()) {
      for (Field field : type.getRawType().getDeclaredFields()) {
        TypeToken<?> fieldType = TypeToken.of(field.getGenericType());
        if (PluginConfig.class.isAssignableFrom(fieldType.getRawType())) {
          // Pick up all config properties
          inspectConfigField(fieldType, result);
          return field.getName();
        }
      }
    }
    return null;
  }

  /**
   * Inspects the plugin config class and build up a map for {@link PluginPropertyField}.
   *
   * @param configType type of the config class
   * @param result map for storing the result
   * @throws UnsupportedTypeException if a field type in the config class is not supported
   */
  private void inspectConfigField(TypeToken<?> configType,
                                  Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    for (TypeToken<?> type : configType.getTypes().classes()) {
      if (PluginConfig.class.equals(type.getRawType())) {
        break;
      }

      for (Field field : type.getRawType().getDeclaredFields()) {
        PluginPropertyField property = createPluginProperty(field, type);
        if (result.containsKey(property.getName())) {
          throw new IllegalArgumentException("Plugin config with name " + property.getName()
                                               + " already defined in " + configType.getRawType());
        }
        result.put(property.getName(), property);
      }
    }
  }

  /**
   * Creates a {@link PluginPropertyField} based on the given field.
   */
  private PluginPropertyField createPluginProperty(Field field,
                                                   TypeToken<?> resolvingType) throws UnsupportedTypeException {
    TypeToken<?> fieldType = resolvingType.resolveType(field.getGenericType());
    Class<?> rawType = fieldType.getRawType();

    Name nameAnnotation = field.getAnnotation(Name.class);
    Description descAnnotation = field.getAnnotation(Description.class);
    String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
    String description = descAnnotation == null ? "" : descAnnotation.value();

    if (rawType.isPrimitive()) {
      return new PluginPropertyField(name, description, rawType.getName(), true);
    }

    rawType = Primitives.unwrap(rawType);
    if (!rawType.isPrimitive() && !String.class.equals(rawType)) {
      throw new UnsupportedTypeException("Only primitive and String types are supported");
    }

    boolean required = true;
    for (Annotation annotation : field.getAnnotations()) {
      if (annotation.annotationType().getName().endsWith(".Nullable")) {
        required = false;
        break;
      }
    }

    return new PluginPropertyField(name, description, rawType.getSimpleName().toLowerCase(), required);
  }

  /**
   * A {@link ClassLoader} that implements {@link Closeable} for resource cleanup. All classloading is done
   * by the delegate {@link ClassLoader}.
   */
  private static final class CloseableClassLoader extends ClassLoader implements Closeable {

    private final Closeable closeable;

    public CloseableClassLoader(ClassLoader delegate, Closeable closeable) {
      super(delegate);
      this.closeable = closeable;
    }

    @Override
    public void close() throws IOException {
      closeable.close();
    }
  }

  /**
   * A {@link Comparator} that uses {@link PluginInfo} to perform comparison.
   */
  private static final class PluginInfoComaprator implements Comparator<PluginInfo> {

    @Override
    public int compare(PluginInfo first, PluginInfo second) {
      return first.compareTo(second);
    }
  }

  /**
   * A {@link Comparator} for {@link PluginClass} that only compares with plugin type and name.
   */
  private static final class PluginClassComparator implements Comparator<PluginClass> {

    @Override
    public int compare(PluginClass first, PluginClass second) {
      int cmp = first.getType().compareTo(second.getType());
      if (cmp != 0) {
        return cmp;
      }
      return first.getName().compareTo(second.getName());
    }
  }

  /**
   * A Gson deserialization for creating {@link PluginClass} object from external plugin config file.
   */
  private static final class PluginClassDeserializer implements JsonDeserializer<PluginClass> {

    // Type for the PluginClass.properties map.
    private static final Type PROPERTIES_TYPE = new TypeToken<Map<String, PluginPropertyField>>() { }.getType();

    @Override
    public PluginClass deserialize(JsonElement json, Type typeOfT,
                                   JsonDeserializationContext context) throws JsonParseException {
      if (!json.isJsonObject()) {
        throw new JsonParseException("Expects json object");
      }

      JsonObject jsonObj = json.getAsJsonObject();

      String type = jsonObj.has("type") ? jsonObj.get("type").getAsString() : Plugin.DEFAULT_TYPE;
      String name = getRequired(jsonObj, "name").getAsString();
      String description = getRequired(jsonObj, "description").getAsString();
      String className = getRequired(jsonObj, "className").getAsString();

      Map<String, PluginPropertyField> properties = jsonObj.has("properties")
        ? context.<Map<String, PluginPropertyField>>deserialize(jsonObj.get("properties"), PROPERTIES_TYPE)
        : ImmutableMap.<String, PluginPropertyField>of();

      return new PluginClass(type, name, description, className, null, properties);
    }

    private JsonElement getRequired(JsonObject jsonObj, String name) {
      if (!jsonObj.has(name)) {
        throw new JsonParseException("Property '" + name + "' is missing");
      }
      return jsonObj.get(name);
    }
  }
}
