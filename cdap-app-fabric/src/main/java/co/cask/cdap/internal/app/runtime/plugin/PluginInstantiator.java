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

package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.lang.FieldVisitor;
import co.cask.cdap.internal.lang.Fields;
import co.cask.cdap.internal.lang.Reflections;
import com.google.common.base.Defaults;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * This class helps creating new instances of plugins. It also contains a ClassLoader cache to
 * save ClassLoader creation.
 *
 * This class implements {@link Closeable} as well for cleanup of temporary directories created for the ClassLoaders.
 */
public class PluginInstantiator implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PluginInstantiator.class);
  // used for setting defaults of string and non-string macro-enabled properties at config time
  private static final Map<String, Class> PROPERTY_TYPES = ImmutableMap.<String, Class>builder()
    .put("boolean", boolean.class)
    .put("byte", byte.class)
    .put("char", char.class)
    .put("double", double.class)
    .put("int", int.class)
    .put("float", float.class)
    .put("long", long.class)
    .put("short", short.class)
    .put("string", String.class)
    .build();

  private final LoadingCache<ArtifactId, ClassLoader> classLoaders;
  private final InstantiatorFactory instantiatorFactory;
  private final File tmpDir;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;

  public PluginInstantiator(CConfiguration cConf, ClassLoader parentClassLoader, File pluginDir) {
    this.instantiatorFactory = new InstantiatorFactory(false);
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();

    this.pluginDir = pluginDir;
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.classLoaders = CacheBuilder.newBuilder()
      .removalListener(new ClassLoaderRemovalListener())
      .build(new ClassLoaderCacheLoader());
    this.parentClassLoader = PluginClassLoader.createParent(parentClassLoader);
  }

  /**
   * Adds a artifact Jar present at the given {@link Location} to allow Plugin Instantiator to load the class
   *
   * @param artifactLocation Location of the Artifact JAR
   * @param destArtifact {@link ArtifactId} of the plugin
   * @throws IOException if failed to copy the artifact JAR
   */
  public void addArtifact(Location artifactLocation, ArtifactId destArtifact) throws IOException {
    File destFile = new File(pluginDir, Artifacts.getFileName(destArtifact));
    Files.copy(Locations.newInputSupplier(artifactLocation), destFile);
  }

  /**
   * Returns a {@link ClassLoader} for the given artifact.
   *
   * @param artifactId {@link ArtifactId}
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public ClassLoader getArtifactClassLoader(ArtifactId artifactId) throws IOException {
    try {
      return classLoaders.get(artifactId);
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Loads and returns the {@link Class} of the given plugin.
   *
   * @param plugin {@link Plugin}
   * @param <T> Type of the plugin
   * @return the plugin Class
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  @SuppressWarnings("unchecked")
  public <T> Class<T> loadClass(Plugin plugin) throws IOException, ClassNotFoundException {
    return (Class<T>) getArtifactClassLoader(plugin.getArtifactId()).loadClass(plugin.getPluginClass().getClassName());
  }

  /**
   * Create a new instance of plugin class without any config, config will be null in the instantiated plugin.
   * @param artifact artifact of the plugin
   * @param pluginClass information about plugin class
   * @param <T> Type of plugin
   * @return a new plugin instance
   */
  public <T> T newInstanceWithoutConfig(ArtifactId artifact,
                                        PluginClass pluginClass) throws IOException, ClassNotFoundException {
    ClassLoader pluginClassLoader = getArtifactClassLoader(artifact);
    Class pluginClassLoaded = pluginClassLoader.loadClass(pluginClass.getClassName());
    return (T) instantiatorFactory.get(TypeToken.of(pluginClassLoaded)).create();
  }

  /**
   * Creates a new instance of the given plugin class.
   * @param plugin {@link Plugin}
   * @param <T> Type of the plugin
   * @return a new plugin instance with macros substituted
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  public <T> T newInstance(Plugin plugin) throws IOException, ClassNotFoundException, InvalidMacroException {
    return newInstance(plugin, null);
  }

  /**
   * Creates a new instance of the given plugin class with all property macros substituted if a MacroEvaluator is given.
   * At runtime, plugin property fields that are macro-enabled and contain macro syntax will remain in the macroFields
   * set in the plugin config.
   * @param plugin {@link Plugin}
   * @param macroEvaluator the MacroEvaluator that performs macro substitution
   * @param <T> Type of the plugin
   * @return a new plugin instance with macros substituted
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   */
  public <T> T newInstance(Plugin plugin, @Nullable MacroEvaluator macroEvaluator)
    throws IOException, ClassNotFoundException, InvalidMacroException {
    ClassLoader classLoader = getArtifactClassLoader(plugin.getArtifactId());
    PluginClass pluginClass = plugin.getPluginClass();
    TypeToken<?> pluginType = TypeToken.of(classLoader.loadClass(pluginClass.getClassName()));

    try {
      String configFieldName = pluginClass.getConfigFieldName();
      // Plugin doesn't have config. Simply return a new instance.
      if (configFieldName == null) {
        return (T) instantiatorFactory.get(pluginType).create();
      }

      // Create the config instance
      Field field = Fields.findField(pluginType.getType(), configFieldName);
      TypeToken<?> configFieldType = pluginType.resolveType(field.getGenericType());
      Object config = instantiatorFactory.get(configFieldType).create();

      // perform macro substitution if an evaluator is provided
      PluginProperties pluginProperties = substituteMacros(plugin, macroEvaluator);
      Reflections.visit(config, configFieldType.getType(),
                        new ConfigFieldSetter(pluginClass, plugin.getArtifactId(), pluginProperties,
                                              getFieldsWithMacro(plugin)));

      // Create the plugin instance
      return newInstance(pluginType, field, configFieldType, config);
    } catch (NoSuchFieldException e) {
      throw new InvalidPluginConfigException("Config field not found in plugin class: " + pluginClass, e);
    } catch (IllegalAccessException e) {
      throw new InvalidPluginConfigException("Failed to set plugin config field: " + pluginClass, e);
    }
  }

  private PluginProperties substituteMacros(Plugin plugin, @Nullable MacroEvaluator macroEvaluator) {
    Map<String, String> properties = new HashMap<>();
    Map<String, PluginPropertyField> pluginPropertyFieldMap = plugin.getPluginClass().getProperties();

    // create macro evaluator and parser based on if it is config or runtime
    boolean configTime = (macroEvaluator == null);
    TrackingMacroEvaluator trackingMacroEvaluator = new TrackingMacroEvaluator();
    MacroParser macroParser = new MacroParser(configTime ? trackingMacroEvaluator : macroEvaluator);

    for (Map.Entry<String, String> property : plugin.getProperties().getProperties().entrySet()) {
      PluginPropertyField field = pluginPropertyFieldMap.get(property.getKey());
      String propertyValue = property.getValue();
      if (field != null && field.isMacroSupported()) {
        // TODO: cleanup after endpoint to get plugin details is merged (#6089)
        if (configTime) {
          // parse for syntax check and check if trackingMacroEvaluator finds macro syntax present
          macroParser.parse(propertyValue);
          propertyValue = getOriginalOrDefaultValue(propertyValue, property.getKey(), field.getType(),
                                                    trackingMacroEvaluator);
        } else {
          propertyValue = macroParser.parse(propertyValue);
        }
      }
      properties.put(property.getKey(), propertyValue);
    }
    return PluginProperties.builder().addAll(properties).build();
  }

  private String getOriginalOrDefaultValue(String originalPropertyString, String propertyName, String propertyType,
                                           TrackingMacroEvaluator trackingMacroEvaluator) {
    if (trackingMacroEvaluator.hasMacro()) {
      trackingMacroEvaluator.reset();
      return getDefaultProperty(propertyName, propertyType);
    }
    return originalPropertyString;
  }

  private String getDefaultProperty(String propertyName, String propertyType) {
    Class propertyClass = PROPERTY_TYPES.get(propertyType);
    if (propertyClass == null) {
      throw new IllegalArgumentException(String.format("Unable to get default value for property %s of type %s.",
                                                       propertyName, propertyType));
    }
    Object defaultProperty = Defaults.defaultValue(propertyClass);
    return defaultProperty == null ? null : defaultProperty.toString();
  }

  private Set<String> getFieldsWithMacro(Plugin plugin) {
    // TODO: cleanup after endpoint to get plugin details is merged (#6089)
    Set<String> macroFields = new HashSet<>();
    Map<String, PluginPropertyField> pluginPropertyFieldMap = plugin.getPluginClass().getProperties();

    TrackingMacroEvaluator trackingMacroEvaluator = new TrackingMacroEvaluator();
    MacroParser macroParser = new MacroParser(trackingMacroEvaluator);

    for (Map.Entry<String, PluginPropertyField> pluginEntry : pluginPropertyFieldMap.entrySet()) {
      if (pluginEntry.getValue() != null && pluginEntry.getValue().isMacroSupported()) {
        String macroValue = plugin.getProperties().getProperties().get(pluginEntry.getKey());
        if (macroValue != null) {
          macroParser.parse(macroValue);
          if (trackingMacroEvaluator.hasMacro()) {
            macroFields.add(pluginEntry.getKey());
            trackingMacroEvaluator.reset();
          }
        }
      }
    }
    return macroFields;
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
    // Cleanup the ClassLoader cache and the temporary directory for the expanded plugin jar.
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
  private final class ClassLoaderCacheLoader extends CacheLoader<ArtifactId, ClassLoader> {

    @Override
    public ClassLoader load(ArtifactId artifactId) throws Exception {
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      File artifact = new File(pluginDir, Artifacts.getFileName(artifactId));
      BundleJarUtil.unJar(Locations.toLocation(artifact), unpackedDir);
      return new PluginClassLoader(unpackedDir, parentClassLoader);
    }
  }

  /**
   * A RemovalListener for closing plugin ClassLoader.
   */
  private static final class ClassLoaderRemovalListener implements RemovalListener<ArtifactId, ClassLoader> {

    @Override
    public void onRemoval(RemovalNotification<ArtifactId, ClassLoader> notification) {
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
    private final ArtifactId artifactId;
    private final Set<String> macroFields;

    ConfigFieldSetter(PluginClass pluginClass, ArtifactId artifactId,
                             PluginProperties properties, Set<String> macroFields) {
      this.pluginClass = pluginClass;
      this.artifactId = artifactId;
      this.properties = properties;
      this.macroFields = macroFields;
    }

    @Override
    public void visit(Object instance, Type inspectType, Type declareType, Field field) throws Exception {
      int modifiers = field.getModifiers();
      if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers) || field.isSynthetic()) {
        return;
      }

      TypeToken<?> declareTypeToken = TypeToken.of(declareType);

      if (PluginConfig.class.equals(declareTypeToken.getRawType())) {
        if (field.getName().equals("properties")) {
          field.set(instance, properties);
        } else if (field.getName().equals("macroFields")) {
          field.set(instance, macroFields);
        }
        return;
      }

      Name nameAnnotation = field.getAnnotation(Name.class);
      String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
      PluginPropertyField pluginPropertyField = pluginClass.getProperties().get(name);
      if (pluginPropertyField.isRequired() && !properties.getProperties().containsKey(name)) {
        throw new IllegalArgumentException("Missing required plugin property " + name
                                             + " for " + pluginClass.getName() + " in artifact " + artifactId);
      }
      String value = properties.getProperties().get(name);
      if (pluginPropertyField.isRequired() || value != null) {
        field.set(instance, convertValue(declareTypeToken.resolveType(field.getGenericType()), value));
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

      if (Character.class.equals(rawType)) {
        if (value.length() != 1) {
          throw new InvalidPluginConfigException(String.format("Property of type char is not length 1: '%s'", value));
        } else {
          return value.charAt(0);
        }
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
}
