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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import co.cask.cdap.internal.artifact.ArtifactVersion;
import co.cask.cdap.internal.lang.FieldVisitor;
import co.cask.cdap.internal.lang.Fields;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;

/**
 * This class helps creating new instances of plugins. It also contains a ClassLoader cache to
 * save ClassLaoder creation.
 *
 * This class implements {@link Closeable} as well for cleanup of temporary directories created for the ClassLoaders.
 */
public class PluginInstantiator implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(PluginInstantiator.class);

  private final LoadingCache<ArtifactDescriptor, ClassLoader> classLoaders;
  private final InstantiatorFactory instantiatorFactory;
  private final File tmpDir;
  private final ClassLoader parentClassLoader;
  // TODO: remove when Templates are removed
  private final File pluginDir;

  // TODO: remove when Templates are removed.
  // This is used by PluginRepository, which works on ApplicationTemplates
  public PluginInstantiator(CConfiguration cConf, String template, ClassLoader parentClassLoader) {
    this.instantiatorFactory = new InstantiatorFactory(false);
    this.pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR), template);

    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.classLoaders = CacheBuilder.newBuilder()
                                    .removalListener(new ClassLoaderRemovalListener())
                                    .build(new ClassLoaderCacheLoader());
    this.parentClassLoader = PluginClassLoader.createParent(new File(pluginDir, "lib"), parentClassLoader);
  }

  // This is used by ArtifactRepository
  public PluginInstantiator(CConfiguration cConf, ClassLoader parentClassLoader) {
    this.instantiatorFactory = new InstantiatorFactory(false);
    // unused in this mode
    this.pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));

    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.classLoaders = CacheBuilder.newBuilder()
                                    .removalListener(new ClassLoaderRemovalListener())
                                    .build(new ClassLoaderCacheLoader());
    this.parentClassLoader = PluginClassLoader.createParent(parentClassLoader);
  }

  /**
   * Returns a {@link ClassLoader} for the given artifact.
   *
   * @param artifactDescriptor descriptor for the artifact
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public ClassLoader getArtifactClassLoader(ArtifactDescriptor artifactDescriptor) throws IOException {
    try {
      return classLoaders.get(artifactDescriptor);
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Get the classloader for the given plugin file.
   *
   * @param pluginInfo information about the plugin file to get the classloader for
   * @return the classloader used to load plugins from the given plugin file
   * @throws IOException if there was an exception creating the classloader, since it may require unpacking jars
   */
  public ClassLoader getPluginClassLoader(PluginInfo pluginInfo) throws IOException {
    return getArtifactClassLoader(getDescriptor(pluginInfo));
  }

  /**
   * Returns the common parent ClassLoader for all plugin ClassLoader created through this class.
   */
  public ClassLoader getParentClassLoader() {
    return parentClassLoader;
  }

  /**
   * Loads and returns the {@link Class} of the given plugin class.
   * TODO: remove once PluginRepository is gone
   *
   * @param pluginInfo information about the plugin. It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class
   * @param <T> Type of the plugin
   * @return the plugin Class
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> loadClass(PluginInfo pluginInfo,
                                PluginClass pluginClass) throws IOException, ClassNotFoundException {
    return loadClass(getDescriptor(pluginInfo), pluginClass);
  }

  /**
   * Loads and returns the {@link Class} of the given plugin class.
   *
   * @param artifactDescriptor descriptor for the artifact the plugin is from.
   *                           It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class
   * @param <T> Type of the plugin
   * @return the plugin Class
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> loadClass(ArtifactDescriptor artifactDescriptor,
                                PluginClass pluginClass) throws IOException, ClassNotFoundException {
    return (Class<T>) getArtifactClassLoader(artifactDescriptor).loadClass(pluginClass.getClassName());
  }

  /**
   * Creates a new instance of the given plugin class.
   * TODO: remove once PluginRepository is gone
   *
   * @param pluginInfo information about the plugin. It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class. The plugin instance will be instantiated based on this.
   * @param properties properties to populate into the {@link PluginConfig} of the plugin instance
   * @param <T> Type of the plugin
   * @return a new plugin instance
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> T newInstance(PluginInfo pluginInfo, PluginClass pluginClass,
                           PluginProperties properties) throws IOException, ClassNotFoundException {
    return newInstance(getDescriptor(pluginInfo), pluginClass, properties);
  }

  /**
   * Creates a new instance of the given plugin class.
   *
   * @param artifactDescriptor descriptor for the artifact the plugin is from.
   *                           It is used for creating the ClassLoader for the plugin.
   * @param pluginClass information about the plugin class. The plugin instance will be instantiated based on this.
   * @param properties properties to populate into the {@link PluginConfig} of the plugin instance
   * @param <T> Type of the plugin
   * @return a new plugin instance
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> T newInstance(ArtifactDescriptor artifactDescriptor, PluginClass pluginClass,
                           PluginProperties properties) throws IOException, ClassNotFoundException {
    ClassLoader classLoader = getArtifactClassLoader(artifactDescriptor);
    TypeToken<?> pluginType = TypeToken.of(classLoader.loadClass(pluginClass.getClassName()));

    try {
      String configFieldName = pluginClass.getConfigFieldName();
      // Plugin doesn't have config. Simply return a new instance.
      if (configFieldName == null) {
        return (T) instantiatorFactory.get(pluginType).create();
      }

      // Create the config instance
      Field field = Fields.findField(pluginType, configFieldName);
      TypeToken<?> configFieldType = pluginType.resolveType(field.getGenericType());
      Object config = instantiatorFactory.get(configFieldType).create();
      Reflections.visit(config, configFieldType, new ConfigFieldSetter(pluginClass, artifactDescriptor, properties));

      // Create the plugin instance
      return newInstance(pluginType, field, configFieldType, config);
    } catch (NoSuchFieldException e) {
      throw new InvalidPluginConfigException("Config field not found in plugin class: " + pluginClass, e);
    } catch (IllegalAccessException e) {
      throw new InvalidPluginConfigException("Failed to set plugin config field: " + pluginClass, e);
    }
  }

  /**
   * Creates a new plugin instance and optionally setup the {@link PluginConfig} field.
   */
  @SuppressWarnings("unchecked")
  private <T> T newInstance(TypeToken<?> pluginType, Field configField,
                            TypeToken<?> configFieldType, Object config) throws IllegalAccessException {
    // See if the plugin has a constructor that takes the config type.
    // Need to loop because we need to resolve the constructor parameter type from generic.
    for (Constructor<?> constructor : pluginType.getRawType().getConstructors()) {
      Type[] parameterTypes = constructor.getGenericParameterTypes();
      if (parameterTypes.length != 1) {
        continue;
      }
      if (configFieldType.equals(pluginType.resolveType(parameterTypes[0]))) {
        constructor.setAccessible(true);
        try {
          // Call the plugin constructor to construct the instance
          return (T) constructor.newInstance(config);
        } catch (Exception e) {
          // Failed to instantiate. Resort to field injection
          LOG.warn("Failed to invoke plugin constructor {}. Resort to config field injection.", constructor);
          break;
        }
      }
    }

    // No matching constructor found, do field injection.
    T plugin = (T) instantiatorFactory.get(pluginType).create();
    configField.setAccessible(true);
    configField.set(plugin, config);
    return plugin;
  }

  @Override
  public void close() throws IOException {
    // Cleanup the ClassLoader cache and the temporary directoy for the expanded plugin jar.
    classLoaders.invalidateAll();
    if (parentClassLoader instanceof Closeable) {
      Closeables.closeQuietly((Closeable) parentClassLoader);
    }
    try {
      DirUtils.deleteDirectoryContents(tmpDir);
    } catch (IOException e) {
      // It's the cleanup step. Nothing much can be done if cleanup failed.
      LOG.warn("Failed to delete directory {}", tmpDir);
    }
  }

  /**
   * A CacheLoader for creating plugin ClassLoader.
   */
  private final class ClassLoaderCacheLoader extends CacheLoader<ArtifactDescriptor, ClassLoader> {

    @Override
    public ClassLoader load(ArtifactDescriptor key) throws Exception {
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.unpackProgramJar(key.getLocation(), unpackedDir);

      return new PluginClassLoader(unpackedDir, parentClassLoader);
    }
  }

  /**
   * A RemovalListener for closing plugin ClassLoader.
   */
  private static final class ClassLoaderRemovalListener implements RemovalListener<ArtifactDescriptor, ClassLoader> {

    @Override
    public void onRemoval(RemovalNotification<ArtifactDescriptor, ClassLoader> notification) {
      ClassLoader cl = notification.getValue();
      if (cl instanceof Closeable) {
        Closeables.closeQuietly((Closeable) cl);
      }
    }
  }

  /**
   * A {@link FieldVisitor} for setting values into {@link PluginConfig} object based on {@link PluginProperties}.
   */
  private static final class ConfigFieldSetter extends FieldVisitor {
    private final PluginClass pluginClass;
    private final PluginProperties properties;
    private final ArtifactDescriptor artifactDescriptor;

    public ConfigFieldSetter(PluginClass pluginClass, ArtifactDescriptor artifactDescriptor,
                             PluginProperties properties) {
      this.pluginClass = pluginClass;
      this.artifactDescriptor = artifactDescriptor;
      this.properties = properties;
    }

    @Override
    public void visit(Object instance, TypeToken<?> inspectType,
                      TypeToken<?> declareType, Field field) throws Exception {
      if (Config.class.equals(declareType.getRawType())) {
        if (field.getName().equals("properties")) {
          field.set(instance, properties);
        }
        return;
      }

      Name nameAnnotation = field.getAnnotation(Name.class);
      String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
      PluginPropertyField pluginPropertyField = pluginClass.getProperties().get(name);
      if (pluginPropertyField.isRequired() && !properties.getProperties().containsKey(name)) {
        throw new IllegalArgumentException("Missing required plugin property " + name
                                             + " for " + pluginClass.getName() + " in artifact " + artifactDescriptor);
      }
      String value = properties.getProperties().get(name);
      if (pluginPropertyField.isRequired() || value != null) {
        field.set(instance, convertValue(declareType.resolveType(field.getGenericType()), value));
      }
    }

    /**
     * Converts string value into value of the fieldType.
     */
    private Object convertValue(TypeToken<?> fieldType, String value) throws Exception {
      // Currently we only support primitive, wrapped primitive and String types.
      Class<?> rawType = fieldType.getRawType();

      if (String.class.equals(rawType)) {
        return value;
      }

      if (rawType.isPrimitive()) {
        rawType = Primitives.wrap(rawType);
      }

      if (Primitives.isWrapperType(rawType)) {
        Method valueOf = rawType.getMethod("valueOf", String.class);
        try {
          return valueOf.invoke(null, value);
        } catch (InvocationTargetException e) {
          if (e.getCause() instanceof NumberFormatException) {
            // if exception is due to wrong value for integer/double conversion
            throw new InvalidPluginConfigException(String.format("valueOf operation on %s failed", value),
                                                   e.getCause());
          }
          throw e;
        }
      }

      throw new UnsupportedTypeException("Only primitive and String types are supported");
    }
  }

  private ArtifactDescriptor getDescriptor(PluginInfo pluginInfo) {
    File file = new File(pluginDir, pluginInfo.getFileName());
    return new ArtifactDescriptor(
      pluginInfo.getName(), new ArtifactVersion(pluginInfo.getVersion().getVersion()), true, Locations.toLocation(file)
    );
  }
}
