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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.artifact.ArtifactId;
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
import io.cdap.cdap.common.lang.jar.ClassLoaderFolder;
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
import java.nio.file.Files;
import java.nio.file.Paths;
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
import java.util.stream.Collectors;
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
  private static final Type MAP_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final LoadingCache<ClassLoaderKey, PluginClassLoader> classLoaders;
  private final InstantiatorFactory instantiatorFactory;
  private final File tmpDir;
  private final File pluginDir;
  private final ClassLoader parentClassLoader;
  private final boolean ownedParentClassLoader;
  private final Gson gson;

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
    // Don't use a static Gson object to avoid caching of classloader, which can cause classloader leakage.
    this.gson = new GsonBuilder().setFieldNamingStrategy(new PluginFieldNamingStrategy()).create();
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
      if ("file".equals(artifactLocation.toURI().getScheme()) && artifactLocation.isDirectory()) {
        Files.createSymbolicLink(destFile.toPath(), Paths.get(artifactLocation.toURI()));
      } else {
        Locations.linkOrCopy(artifactLocation, destFile);
      }
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
  public PluginClassLoader getArtifactClassLoader(ArtifactId artifactId) throws IOException {
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
    return newInstance(plugin, macroEvaluator, null);
  }

  /**
   * Creates a new instance of the given plugin class with all property macros substituted if a MacroEvaluator is given.
   * At runtime, plugin property fields that are macro-enabled and contain macro syntax will remain in the macroFields
   * set in the plugin config.
   * @param plugin {@link Plugin}
   * @param macroEvaluator the MacroEvaluator that performs macro substitution
   * @param options macro parser options
   * @param <T> Type of the plugin
   * @return a new plugin instance with macros substituted
   * @throws IOException if failed to expand the plugin jar to create the plugin ClassLoader
   * @throws ClassNotFoundException if failed to load the given plugin class
   * @throws InvalidPluginConfigException if the PluginConfig could not be created from the plugin properties
   */
  public <T> T newInstance(
    Plugin plugin, @Nullable MacroEvaluator macroEvaluator,
    @Nullable MacroParserOptions options) throws IOException, ClassNotFoundException, InvalidMacroException {
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
      PluginProperties pluginProperties = substituteMacros(plugin, macroEvaluator, options);
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

  public PluginProperties substituteMacros(Plugin plugin, @Nullable MacroEvaluator macroEvaluator,
                                           @Nullable MacroParserOptions options) {
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

          // if the field is a nested field and it has macro in it, there are two scenarios:
          // 1. the field itself needs to get evaluated, for example, ${conn(test)}
          // 2. the field itself is already a map json string, but some field inside the map is a macro, for example,
          //    secure macros.
          if (!field.getChildren().isEmpty() && trackingMacroEvaluator.hasMacro()) {
            try {
              Map<String, String> childMap = gson.fromJson(propertyValue, MAP_STRING_TYPE);
              // if this is already a map, this is scenario 2, we need to get the default value for the field
              // one by one depending on if there is a macro in it
              trackingMacroEvaluator.reset();
              Map<String, String> substitutedChildMap = new HashMap<>();
              childMap.forEach((name, value) -> {
                macroParser.parse(value);
                substitutedChildMap.put(name, getOriginalOrDefaultValue(
                  value, name, pluginPropertyFieldMap.get(name).getType(), trackingMacroEvaluator));
              });
              propertyValue = gson.toJson(substitutedChildMap);
            } catch (JsonSyntaxException e) {
              // this is scenario 1, just continue
            }
          }
          propertyValue = getOriginalOrDefaultValue(propertyValue, property.getKey(), field.getType(),
                                                    trackingMacroEvaluator);
        } else {
          MacroParserOptions parserOptions = options == null ? MacroParserOptions.builder()
                                                                 .setEscaping(field.isMacroEscapingEnabled())
                                                                 .build() : options;
          MacroParser macroParser = new MacroParser(macroEvaluator, parserOptions);
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
      return getDefaultProperty(propertyType);
    }
    return originalPropertyString;
  }

  private String getDefaultProperty(String propertyType) {
    Class<?> propertyClass = PROPERTY_TYPES.get(propertyType);
    if (propertyClass == null) {
      return null;
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

            if (pluginField.getChildren().isEmpty()) {
              continue;
            }

            // if the field is a nested field and it has macro in it, there are two scenarios:
            // 1. the field itself needs to get evaluated, for example, ${conn(test)}
            // 2. the field itself is already a map json string, but some field inside the map is a macro, for example,
            //    secure macros.
            try {
              Map<String, String> childMap = gson.fromJson(macroValue, MAP_STRING_TYPE);
              // if this is already a map, this is scenario 2, we need to check the fields one by one
              macroFields.remove(pluginEntry.getKey());
              childMap.forEach((name, value) -> {
                if (value != null) {
                  macroParser.parse(value);
                  if (trackingMacroEvaluator.hasMacro()) {
                    macroFields.add(name);
                  }
                  trackingMacroEvaluator.reset();
                }
              });
            } catch (JsonSyntaxException e) {
              // this is scenario 1, then mark all the fields inside the field as macro
              macroFields.addAll(pluginField.getChildren());
            }
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
      File artifact = new File(pluginDir, Artifacts.getFileName(key.artifact));
      ClassLoaderFolder classLoaderFolder = BundleJarUtil.prepareClassLoaderFolder(
        Locations.toLocation(artifact), () -> DirUtils.createTempDir(tmpDir));

      Iterator<ArtifactId> parentIter = key.parents.iterator();
      if (!parentIter.hasNext()) {
        return new PluginClassLoader(key.artifact, classLoaderFolder.getDir(),
                                     artifact.getAbsolutePath(), parentClassLoader);
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
      return new PluginClassLoader(key.artifact, classLoaderFolder.getDir(), artifact.getAbsolutePath(), parentCL);
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
    private final Gson gson;

    ConfigFieldSetter(PluginClass pluginClass, PluginProperties properties, PluginProperties rawProperties,
                      Set<String> macroFields) {
      this.pluginClass = pluginClass;
      this.properties = properties;
      this.rawProperties = rawProperties;
      this.macroFields = macroFields;
      this.missingProperties = new HashSet<>();
      this.invalidProperties = new HashSet<>();

      // Don't use a static Gson object to avoid caching of classloader, which can cause classloader leakage.
      this.gson = new GsonBuilder().setFieldNamingStrategy(new PluginFieldNamingStrategy()).create();
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
      // if the property is required and it's not a macro and the property doesn't exist and it is not an config
      // that is consisted of a collection of configs
      Set<String> children = pluginPropertyField.getChildren();
      if (pluginPropertyField.isRequired()
            && !macroFields.contains(name)
            && properties.getProperties().get(name) == null
            && children.isEmpty()) {
        missingProperties.add(name);
        return;
      }

      String value = properties.getProperties().get(name);

      // if the value is null but this field is consisted of children, look up all the child properties to build
      // the config
      if (value == null && !children.isEmpty()) {
        Map<String, String> childProperties = new HashMap<>();
        boolean missing = false;
        for (String child : children) {
          PluginPropertyField childProperty = pluginClass.getProperties().get(child);
          // if child property is required and it is missing, add it to missing properties and continue
          if (childProperty.isRequired() && !macroFields.contains(child) &&
                !properties.getProperties().containsKey(child)) {
            missingProperties.add(child);
            missing = true;
            continue;
          }
          childProperties.put(child, properties.getProperties().get(child));
        }

        // if missing any required field, return here
        if (missing) {
          return;
        }
        value = gson.toJson(childProperties);
      }

      if (pluginPropertyField.isRequired() || value != null) {
        try {
          Object convertedValue = convertValue(name, declareType,
                                               declareTypeToken.resolveType(field.getGenericType()), value);

          // set the remaining plugin properties field
          if (!children.isEmpty() && convertedValue instanceof PluginConfig) {
            PluginConfig config = (PluginConfig) convertedValue;
            setChildPluginConfigField(config, "properties", PluginProperties.builder().addAll(
              properties.getProperties().entrySet().stream()
                .filter(entry -> children.contains(entry.getKey()))
                // Collectors.toMap does not take null entry value, so use HashMap instead
                .collect(HashMap::new, (map, entry)-> map.put(entry.getKey(),
                                                              entry.getValue()), HashMap::putAll)).build());
            setChildPluginConfigField(config, "rawProperties", PluginProperties.builder().addAll(
              rawProperties.getProperties().entrySet().stream()
                .filter(entry -> children.contains(entry.getKey()))
                .collect(HashMap::new, (map, entry)-> map.put(entry.getKey(),
                                                              entry.getValue()), HashMap::putAll)).build());
            setChildPluginConfigField(config, "macroFields",
                                      macroFields.stream().filter(children::contains).collect(Collectors.toSet()));
          }
          field.set(instance, convertedValue);
        } catch (Exception e) {
          invalidProperties.add(new InvalidPluginProperty(name, e));
        }
      }
    }

    private void setChildPluginConfigField(PluginConfig config, String fieldName,
                                           Object fieldVal) throws NoSuchFieldException, IllegalAccessException {
      Field childField = PluginConfig.class.getDeclaredField(fieldName);
      childField.setAccessible(true);
      childField.set(config, fieldVal);
    }

    /**
     * Converts string value into value of the fieldType.
     */
    private Object convertValue(String name, Type declareType, TypeToken<?> fieldType, String value) throws Exception {
      // For primitive, wrapped primitive, and String types, we convert the string value into the corresponding type
      // For object type, we assume the string value is a Json string, and try to use Gson to deserialize into the
      // given field type.
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
              String.format("Value of field %s.%s is null or empty. It should be a number", declareType, name) :
              String.format("Value of field %s.%s is expected to be a number", declareType, name);
            throw new InvalidPluginConfigException(errorMessage, e.getCause());
          }
          throw e;
        }
      }

      try {
        // Assuming it is a POJO type, use Gson to deserialize the value
        return gson.fromJson(value, fieldType.getType());
      } catch (JsonSyntaxException e) {
        throw new InvalidPluginConfigException(
          String.format("Failed to assign value '%s' to plugin config field %s.%s", value, declareType, name), e);
      }
    }
  }
}
