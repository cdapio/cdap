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

import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.api.templates.plugins.PluginVersion;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactClasses;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactInspector;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
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
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * This class manage plugin information that are available for application templates.
 * TODO: remove this class once the artifact repository is ready
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

  private final File pluginDir;
  private final File tmpDir;
  private final AtomicReference<Map<String, TreeMultimap<PluginInfo, PluginClass>>> plugins;
  private final ArtifactInspector artifactInspector;

  @Inject
  PluginRepository(CConfiguration cConf) {
    this.pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));
    this.plugins = new AtomicReference<Map<String, TreeMultimap<PluginInfo, PluginClass>>>(
      new HashMap<String, TreeMultimap<PluginInfo, PluginClass>>());
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.artifactInspector = new ArtifactInspector(cConf);
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
    try (CloseableClassLoader templateClassLoader = createTemplateClassLoader(templateJar)) {
      for (PluginFile pluginFile : pluginFiles) {
        if (!configureByFile(pluginFile, templatePlugins)) {
          try {
            configureByInspection(pluginFile, templateClassLoader, template, templatePlugins);
          } catch (NoClassDefFoundError e) {
            // Continue to configure plugins even if one of them threw a NoClassDefFoundError
            LOG.warn("Error while trying to inspect plugin : {}. Make sure to include plugin config json file if " +
                       "you want to use 3rd party jars as plugins.", pluginFile, e);
          }
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
  private void configureByInspection(PluginFile pluginFile, ClassLoader templateClassLoader, String template,
                                     Multimap<PluginInfo, PluginClass> templatePlugins) throws IOException {

    PluginInfo pluginInfo = pluginFile.getPluginInfo();
    Id.Artifact artifactId = Id.Artifact.from(
      Constants.SYSTEM_NAMESPACE_ID, pluginInfo.getName(), pluginInfo.getVersion().getVersion());

    ArtifactClasses artifactClasses =
      artifactInspector.inspectArtifact(artifactId, pluginFile.getFile(), template, templateClassLoader);
    for (PluginClass pluginClass : artifactClasses.getPlugins()) {
      if (!templatePlugins.put(pluginFile.getPluginInfo(), pluginClass)) {
        LOG.warn("Plugin already exists in {}. Ignore plugin class {}", pluginFile.getPluginInfo(), pluginClass);
      }
    }
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
    final ProgramClassLoader programClassLoader = ProgramClassLoader.create(unpackDir, getClass().getClassLoader());
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() {
        try {
          Closeables.closeQuietly(programClassLoader);
          DirUtils.deleteDirectoryContents(unpackDir);
        } catch (IOException e) {
          LOG.warn("Failed to delete directory {}", unpackDir, e);
        }
      }
    });
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
