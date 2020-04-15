/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import com.google.common.base.Defaults;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.lang.FieldVisitor;
import io.cdap.cdap.internal.lang.Fields;
import io.cdap.cdap.internal.lang.Reflections;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private static final Map<String, Class<?>> PROPERTY_TYPES = ImmutableMap.<String, Class<?>>builder()
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

  private final LoadingCache<ClassLoaderKey, PluginClassLoader> classLoaders;
  private final InstantiatorFactory instantiatorFactory;
  private final File tmpDir;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final boolean ownedParentClassLoader;

  public PluginInstantiator(CConfiguration cConf, ClassLoader parentClassLoader, File pluginDir) {
    this(cConf, parentClassLoader, pluginDir, true);
  }

  public PluginInstantiator(CConfiguration cConf, ClassLoader parentClassLoader, File pluginDir,
                            boolean filterClassloader) {
    this.instantiatorFactory = new InstantiatorFactory(false);
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();

    this.pluginDir = pluginDir;
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.classLoaders = CacheBuilder.newBuilder()
      .removalListener(new ClassLoaderRemovalListener())
      .build(new ClassLoaderCacheLoader());
    this.parentClassLoader = filterClassloader ? PluginClassLoader.createParent(parentClassLoader) : parentClassLoader;
    this.ownedParentClassLoader = filterClassloader;
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
    if (!destFile.exists()) {
      Locations.linkOrCopy(artifactLocation, destFile);
    }
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
      return classLoaders.get(new ClassLoaderKey(artifactId));
    } catch (ExecutionException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Returns a {@link ClassLoader} for the given plugin.
   *
   * @param plugin {@link Plugin}
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public ClassLoader getPluginClassLoader(Plugin plugin) throws IOException {
    return getPluginClassLoader(plugin.getArtifactId(), plugin.getParents());
  }

  /**
   * Returns a {@link ClassLoader} for the given plugin.
   *
   * @param artifactId the artifact id of the plugin
   * @param pluginParents the list of parents' artifact id of the plugin that are also plugins
   * @throws IOException if failed to expand the artifact jar to create the plugin ClassLoader
   *
   * @see PluginClassLoader
   */
  public PluginClassLoader getPluginClassLoader(ArtifactId artifactId,
                                                List<ArtifactId> pluginParents) throws IOException {
    try {
      return classLoaders.get(new ClassLoaderKey(artifactId, pluginParents));
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
    return (Class<T>) getPluginClassLoader(plugin).loadClass(plugin.getPluginClass().getClassName());
  }

  /**
   * Create a new instance of plugin class without any config, config will be null in the instantiated plugin.
   * @param plugin information about the plugin
   * @param <T> Type of plugin
   * @return a new plugin instance
   */
  public <T> T newInstanceWithoutConfig(Plugin plugin) throws IOException, ClassNotFoundException {
    ClassLoader pluginClassLoader = getPluginClassLoader(plugin);
    @SuppressWarnings("unchecked")
    Class<T> pluginClassLoaded = (Class<T>) pluginClassLoader.loadClass(plugin.getPluginClass().getClassName());
    return instantiatorFactory.get(TypeToken.of(pluginClassLoaded)).create();
  }

  /**
   * Creates a new instance of the given plugin class.
   * @param plugin {@link Plugin}
   * @param <T> Type of the plugin
   * @return a new plugin instance with macros substituted
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   * @throws InvalidPluginConfigException if the PluginConfig could not be created from the plugin properties
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
   * @throws InvalidPluginConfigException if the PluginConfig could not be created from the plugin properties
   */
  public <T> T newInstance(Plugin plugin, @Nullable MacroEvaluator macroEvaluator)
    throws IOException, ClassNotFoundException, InvalidMacroException {
    ClassLoader classLoader = getPluginClassLoader(plugin);
    PluginClass pluginClass = plugin.getPluginClass();
    @SuppressWarnings("unchecked")
    TypeToken<T> pluginType = TypeToken.of((Class<T>) classLoader.loadClass(pluginClass.getClassName()));

    try {
      String configFieldName = pluginClass.getConfigFieldName();
      // Plugin doesn't have config. Simply return a new instance.
      if (configFieldName == null) {
        return instantiatorFactory.get(pluginType).create();
      }

      // Create the config instance
      Field field = Fields.findField(pluginType.getType(), configFieldName);
      TypeToken<?> configFieldType = pluginType.resolveType(field.getGenericType());
      Object config = instantiatorFactory.get(configFieldType).create();

      // perform macro substitution if an evaluator is provided, collect fields with macros only at configure time
      PluginProperties pluginProperties = substituteMacros(plugin, macroEvaluator);
      Set<String> macroFields = (macroEvaluator == null) ? getFieldsWithMacro(plugin) : Collections.emptySet();

      PluginProperties rawProperties = plugin.getProperties();
      ConfigFieldSetter fieldSetter = new ConfigFieldSetter(pluginClass, pluginProperties, rawProperties, macroFields);
      Reflections.visit(config, configFieldType.getType(), fieldSetter);

      if (!fieldSetter.invalidProperties.isEmpty() || !fieldSetter.missingProperties.isEmpty()) {
        throw new InvalidPluginConfigException(pluginClass, fieldSetter.missingProperties,
                                               fieldSetter.invalidProperties);
      }

      // Create the plugin instance
      return newInstance(pluginType, field, configFieldType, config);
    } catch (NoSuchFieldException e) {
      throw new InvalidPluginConfigException("Config field not found in plugin class: " + pluginClass, e);
    } catch (IllegalAccessException e) {
      throw new InvalidPluginConfigException("Failed to set plugin config field: " + pluginClass, e);
    }
  }

  public PluginProperties substituteMacros(Plugin plugin, @Nullable MacroEvaluator macroEvaluator) {
    Map<String, String> properties = new HashMap<>();
    Map<String, PluginPropertyField> pluginPropertyFieldMap = plugin.getPluginClass().getProperties();

    // create macro evaluator and parser based on if it is config or runtime
    boolean configTime = (macroEvaluator == null);
    TrackingMacroEvaluator trackingMacroEvaluator = new TrackingMacroEvaluator();

    for (Map.Entry<String, String> property : plugin.getProperties().getProperties().entrySet()) {
      PluginPropertyField field = pluginPropertyFieldMap.get(property.getKey());
      String propertyValue = property.getValue();
      if (field != null && field.isMacroSupported()) {
        // TODO: cleanup after endpoint to get plugin details is merged (#6089)
        if (configTime) {
          // parse for syntax check and check if trackingMacroEvaluator finds macro syntax present
          MacroParser macroParser = new MacroParser(trackingMacroEvaluator,
                                                    MacroParserOptions.builder()
                                                      .setEscaping(field.isMacroEscapingEnabled())
                                                      .build());
          macroParser.parse(propertyValue);
          propertyValue = getOriginalOrDefaultValue(propertyValue, property.getKey(), field.getType(),
                                                    trackingMacroEvaluator);
        } else {
          MacroParser macroParser = new MacroParser(macroEvaluator,
                                                    MacroParserOptions.builder()
                                                      .setEscaping(field.isMacroEscapingEnabled())
                                                      .build());
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
    Class<?> propertyClass = PROPERTY_TYPES.get(propertyType);
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

    for (Map.Entry<String, PluginPropertyField> pluginEntry : pluginPropertyFieldMap.entrySet()) {
      PluginPropertyField pluginField = pluginEntry.getValue();
      if (pluginEntry.getValue() != null && pluginField.isMacroSupported()) {
        String macroValue = plugin.getProperties().getProperties().get(pluginEntry.getKey());
        if (macroValue != null) {
          MacroParser macroParser = new MacroParser(trackingMacroEvaluator,
                                                    MacroParserOptions.builder()
                                                      .setEscaping(pluginField.isMacroEscapingEnabled())
                                                      .build());
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
        } catch (InvocationTargetException e) {
          // If there is exception thrown from the constructor, propagate it.
          throw Throwables.propagate(e.getCause());
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
    if (ownedParentClassLoader) {
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
   * Key for the classloader cache.
   */
  private static class ClassLoaderKey {
    private final List<ArtifactId> parents;
    private final ArtifactId artifact;

    ClassLoaderKey(ArtifactId artifact) {
      this(artifact, Collections.emptyList());
    }

    ClassLoaderKey(ArtifactId artifact, List<ArtifactId> parents) {
      this.parents = parents;
      this.artifact = artifact;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ClassLoaderKey that = (ClassLoaderKey) o;

      return Objects.equals(parents, that.parents) && Objects.equals(artifact, that.artifact);
    }

    @Override
    public int hashCode() {
      return Objects.hash(parents, artifact);
    }
  }

  /**
   * A CacheLoader for creating plugin ClassLoader.
   */
  private final class ClassLoaderCacheLoader extends CacheLoader<ClassLoaderKey, PluginClassLoader> {

    @Override
    public PluginClassLoader load(ClassLoaderKey key) throws Exception {
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      File artifact = new File(pluginDir, Artifacts.getFileName(key.artifact));
      BundleJarUtil.unJar(Locations.toLocation(artifact), unpackedDir);

      Iterator<ArtifactId> parentIter = key.parents.iterator();
      if (!parentIter.hasNext()) {
        return new PluginClassLoader(key.artifact, unpackedDir, parentClassLoader);
      }

      List<ArtifactId> parentsOfParent = new ArrayList<>(key.parents.size() - 1);
      ArtifactId parentArtifact = parentIter.next();
      while (parentIter.hasNext()) {
        parentsOfParent.add(parentIter.next());
      }
      /*
       *   Combine CL [filtered grandparentCL (export-packages only),
       *               filtered parentPluginCL (export-packages only)]
       *                         ^
       *                         |
       *                         |
       *        Plugin CL (classes in plugin artifact)
       *
       * The plugin classloader's parent will have whatever is exported by the parent plugin, and whatever
       * is exported by the grandparent. Today, since we don't allow past a grandparent, the grandparent should
       * always be the filtered program classloader. But if we change it to allow arbitrary levels, the grandparent
       * could be another plugin. In effect, the plugin should have access to everything exported by plugins and apps
       * above it.
       */
      PluginClassLoader parentPluginCL = getPluginClassLoader(parentArtifact, parentsOfParent);
      ClassLoader parentCL =
        new CombineClassLoader(parentPluginCL.getParent(), parentPluginCL.getExportPackagesClassLoader());
      return new PluginClassLoader(key.artifact, unpackedDir, parentCL);
    }
  }

  /**
   * A RemovalListener for closing plugin ClassLoader.
   */
  private static final class ClassLoaderRemovalListener implements RemovalListener<ClassLoaderKey, PluginClassLoader> {

    @Override
    public void onRemoval(RemovalNotification<ClassLoaderKey, PluginClassLoader> notification) {
      Closeables.closeQuietly(notification.getValue());
    }
  }

  /**
   * A {@link FieldVisitor} for setting values into {@link PluginConfig} object based on {@link PluginProperties}.
   */
  private static final class ConfigFieldSetter extends FieldVisitor {
    private final PluginClass pluginClass;
    private final PluginProperties properties;
    private final PluginProperties rawProperties;
    private final Set<String> macroFields;
    private final Set<String> missingProperties;
    private final Set<InvalidPluginProperty> invalidProperties;

    ConfigFieldSetter(PluginClass pluginClass, PluginProperties properties, PluginProperties rawProperties,
                      Set<String> macroFields) {
      this.pluginClass = pluginClass;
      this.properties = properties;
      this.rawProperties = rawProperties;
      this.macroFields = macroFields;
      this.missingProperties = new HashSet<>();
      this.invalidProperties = new HashSet<>();
    }

    @Override
    public void visit(Object instance, Type inspectType, Type declareType, Field field) throws Exception {
      int modifiers = field.getModifiers();
      if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers) || field.isSynthetic()) {
        return;
      }

      TypeToken<?> declareTypeToken = TypeToken.of(declareType);

      if (PluginConfig.class.equals(declareTypeToken.getRawType())) {
        switch (field.getName()) {
          case "properties":
            field.set(instance, properties);
            break;
          case "macroFields":
            field.set(instance, macroFields);
            break;
          case "rawProperties":
            field.set(instance, rawProperties);
            break;
        }
        return;
      }

      Name nameAnnotation = field.getAnnotation(Name.class);
      String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
      PluginPropertyField pluginPropertyField = pluginClass.getProperties().get(name);
      // if the property is required and it's not a macro and the property doesn't exist
      if (pluginPropertyField.isRequired() && !macroFields.contains(name) &&
        properties.getProperties().get(name) == null) {
        missingProperties.add(name);
        return;
      }
      String value = properties.getProperties().get(name);
      if (pluginPropertyField.isRequired() || value != null) {
        try {
          field.set(instance, convertValue(name, declareTypeToken.resolveType(field.getGenericType()), value));
        } catch (Exception e) {
          invalidProperties.add(new InvalidPluginProperty(name, e));
        }
      }
    }

    /**
     * Converts string value into value of the fieldType.
     */
    private Object convertValue(String name, TypeToken<?> fieldType, String value) throws Exception {
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
          throw new IllegalArgumentException(String.format("Property of type char is not length 1: '%s'", value));
        } else {
          return value.charAt(0);
        }
      }

      // discard decimal point for all non floating point data types
      if (Long.class.equals(rawType) || Short.class.equals(rawType)
        || Integer.class.equals(rawType) || Byte.class.equals(rawType)) {
        if (value.endsWith(".0")) {
          value = value.substring(0, value.lastIndexOf("."));
        }
      }

      if (Primitives.isWrapperType(rawType)) {
        Method valueOf = rawType.getMethod("valueOf", String.class);
        try {
          return valueOf.invoke(null, value);
        } catch (InvocationTargetException e) {
          if (e.getCause() instanceof NumberFormatException) {
            // if exception is due to wrong value for integer/double conversion
            String errorMessage = Strings.isNullOrEmpty(value) ?
              String.format("Value of field %s is null or empty. It should be a number", name) :
              String.format("Value of field %s is expected to be a number", name);
            throw new InvalidPluginConfigException(errorMessage, e.getCause());
          }
          throw e;
        }
      }

      throw new UnsupportedTypeException(String.format("Plugin config property '%s' is of invalid type '%s'. " +
                                                         "Only primitive and String types are supported",
                                                       name, rawType.getSimpleName()));
    }
  }
}
