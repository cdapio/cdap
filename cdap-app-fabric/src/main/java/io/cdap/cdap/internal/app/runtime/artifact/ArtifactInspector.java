/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import org.apache.twill.filesystem.Location;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipException;
import javax.annotation.Nullable;

/**
 * Inspects a jar file to determine metadata about the artifact.
 */
final class ArtifactInspector {
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactInspector.class);

  private final CConfiguration cConf;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final ReflectionSchemaGenerator schemaGenerator;

  ArtifactInspector(CConfiguration cConf, ArtifactClassLoaderFactory artifactClassLoaderFactory) {
    this.cConf = cConf;
    this.artifactClassLoaderFactory = artifactClassLoaderFactory;
    this.schemaGenerator = new ReflectionSchemaGenerator(false);
  }

  /**
   * Inspect the given artifact to determine the classes contained in the artifact.
   *
   * @param artifactId the id of the artifact to inspect
   * @param artifactFile the artifact file
   * @param parentClassLoader the parent classloader to use when inspecting plugins contained in the artifact.
   *                          For example, a ProgramClassLoader created from the artifact the input artifact extends
   * @param additionalPlugins Additional plugin classes
   * @return metadata about the classes contained in the artifact
   * @throws IOException if there was an exception opening the jar file
   * @throws InvalidArtifactException if the artifact is invalid. For example, if the application main class is not
   *                                  actually an Application.
   */
  ArtifactClasses inspectArtifact(Id.Artifact artifactId, File artifactFile,
                                  @Nullable ClassLoader parentClassLoader, Set<PluginClass> additionalPlugins)
    throws IOException, InvalidArtifactException {
    Path tmpDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).toAbsolutePath();
    Files.createDirectories(tmpDir);
    Location artifactLocation = Locations.toLocation(artifactFile);

    Path stageDir = Files.createTempDirectory(tmpDir, artifactFile.getName());
    try {
      File unpackedDir = BundleJarUtil.unJar(artifactLocation,
                                             Files.createTempDirectory(stageDir, "unpacked-").toFile());
      try (
        CloseableClassLoader artifactClassLoader = artifactClassLoaderFactory.createClassLoader(unpackedDir);
        PluginInstantiator pluginInstantiator =
          new PluginInstantiator(cConf, parentClassLoader == null ? artifactClassLoader : parentClassLoader,
                                 Files.createTempDirectory(stageDir, "plugins-").toFile(),
                                 false)) {
        pluginInstantiator.addArtifact(artifactLocation, artifactId.toArtifactId());
        ArtifactClasses.Builder builder = inspectApplications(artifactId, ArtifactClasses.builder(),
                                                              artifactLocation, artifactClassLoader);
        return inspectPlugins(builder, artifactFile, artifactId.toArtifactId(), pluginInstantiator,
                              additionalPlugins).build();
      }
    } catch (EOFException | ZipException e) {
      throw new InvalidArtifactException("Artifact " + artifactId + " is not a valid zip file.", e);
    } finally {
      try {
        DirUtils.deleteDirectoryContents(stageDir.toFile());
      } catch (IOException e) {
        LOG.warn("Exception raised while deleting directory {}", stageDir, e);
      }
    }
  }

  private ArtifactClasses.Builder inspectApplications(Id.Artifact artifactId,
                                                      ArtifactClasses.Builder builder,
                                                      Location artifactLocation,
                                                      ClassLoader artifactClassLoader) throws IOException,
                                                                                              InvalidArtifactException {

    // right now we force users to include the application main class as an attribute in their manifest,
    // which forces them to have a single application class.
    // in the future, we may want to let users do this or maybe specify a list of classes or
    // a package that will be searched for applications, to allow multiple applications in a single artifact.
    String mainClassName;
    try {
      Manifest manifest = BundleJarUtil.getManifest(artifactLocation);
      if (manifest == null) {
        return builder;
      }
      Attributes manifestAttributes = manifest.getMainAttributes();
      if (manifestAttributes == null) {
        return builder;
      }
      mainClassName = manifestAttributes.getValue(ManifestFields.MAIN_CLASS);
    } catch (ZipException e) {
      throw new InvalidArtifactException(String.format(
        "Couldn't unzip artifact %s, please check it is a valid jar file.", artifactId), e);
    }

    if (mainClassName == null) {
      return builder;
    }

    try {
      Object appMain = artifactClassLoader.loadClass(mainClassName).newInstance();
      if (!(appMain instanceof Application)) {
        // we don't want to error here, just don't record an application class.
        // possible for 3rd party plugin artifacts to have the main class set
        return builder;
      }

      Application app = (Application) appMain;

      java.lang.reflect.Type configType;
      // if the user parameterized their application, like 'xyz extends Application<T>',
      // we can deserialize the config into that object. Otherwise it'll just be a Config
      try {
        configType = Artifacts.getConfigType(app.getClass());
      } catch (Exception e) {
        throw new InvalidArtifactException(String.format(
          "Could not resolve config type for Application class %s in artifact %s. " +
            "The type must extend Config and cannot be parameterized.", mainClassName, artifactId));
      }

      Schema configSchema = configType == Config.class ? null : schemaGenerator.generate(configType);
      builder.addApp(new ApplicationClass(mainClassName, "", configSchema, getArtifactRequirements(app.getClass())));
    } catch (ClassNotFoundException e) {
      throw new InvalidArtifactException(String.format(
        "Could not find Application main class %s in artifact %s.", mainClassName, artifactId));
    } catch (UnsupportedTypeException e) {
      throw new InvalidArtifactException(String.format(
        "Config for Application %s in artifact %s has an unsupported schema. " +
          "The type must extend Config and cannot be parameterized.", mainClassName, artifactId));
    } catch (InstantiationException | IllegalAccessException e) {
      throw new InvalidArtifactException(String.format(
        "Could not instantiate Application class %s in artifact %s.", mainClassName, artifactId), e);
    }

    return builder;
  }

  /**
   * Inspects the plugin file and extracts plugin classes information.
   */
  private ArtifactClasses.Builder inspectPlugins(ArtifactClasses.Builder builder, File artifactFile,
                                                 ArtifactId artifactId, PluginInstantiator pluginInstantiator,
                                                 Set<PluginClass> additionalPlugins)
    throws IOException, InvalidArtifactException {
    ClassLoader pluginClassLoader = pluginInstantiator.getArtifactClassLoader(artifactId);
    inspectAdditionalPlugins(artifactId, additionalPlugins, pluginClassLoader);

    // See if there are export packages. Plugins should be in those packages
    Set<String> exportPackages = getExportPackages(artifactFile);
    if (exportPackages.isEmpty()) {
      return builder;
    }

    try {
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
                                                    configField, pluginProperties, getArtifactRequirements(cls));
          builder.addPlugin(pluginClass);
        } catch (UnsupportedTypeException e) {
          LOG.warn("Plugin configuration type not supported. Plugin ignored. {}", cls, e);
        }
      }
    } catch (Throwable t) {
      throw new InvalidArtifactException(String.format(
        "Class could not be found while inspecting artifact for plugins. " +
          "Please check dependencies are available, and that the correct parent artifact was specified. " +
          "Error class: %s, message: %s.", t.getClass(), t.getMessage()), t);
    }

    return builder;
  }

  private void inspectAdditionalPlugins(ArtifactId artifactId, Set<PluginClass> additionalPlugins,
                                        ClassLoader pluginClassLoader) throws InvalidArtifactException {
    if (additionalPlugins != null) {
      for (PluginClass pluginClass : additionalPlugins) {
        try {
          // Make sure additional plugin classes can be loaded. This is to ensure that plugin artifacts without
          // plugin classes are not deployed.
          pluginClassLoader.loadClass(pluginClass.getClassName());
        } catch (ClassNotFoundException e) {
          throw new InvalidArtifactException(
            String.format("Artifact %s with version %s and scope %s does not have class %s.",
                          artifactId.getName(), artifactId.getVersion(), artifactId.getScope().name(),
                          pluginClass.getClassName()), e);
        }
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
                List<Iterator<String>> iterators = new ArrayList<>();
                // Go though all available resources and collect all class names that are plugin classes.
                while (resources.hasMoreElements()) {
                  URL packageResource = resources.nextElement();

                  // Only inspect classes in the top level jar file for Plugins.
                  // The jar manifest may have packages in Export-Package that are loadable from the bundled jar files,
                  // which is for classloading purpose. Those classes won't be inspected for plugin classes.
                  // There should be exactly one of resource that match, because it maps to a directory on the FS.
                  if (packageResource.getProtocol().equals("file")) {
                    Iterator<String> classFiles = DirUtils.list(new File(packageResource.toURI()), "class").iterator();

                    // Transform class file into class name and filter by @Plugin class only
                    iterators.add(Iterators.filter(
                      Iterators.transform(classFiles, new Function<String, String>() {
                        @Override
                        public String apply(String input) {
                          return getClassName(currentPackage, input);
                        }
                      }), new Predicate<String>() {
                        @Override
                        public boolean apply(String className) {
                          return isPlugin(className, pluginClassLoader);
                        }
                      }));
                  }
                }
                if (!iterators.isEmpty()) {
                  classIterator = Iterators.concat(iterators.iterator());
                }
              } catch (Exception e) {
                // Cannot happen
                throw Throwables.propagate(e);
              }
            }

            try {
              return pluginClassLoader.loadClass(classIterator.next());
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
              // Cannot happen, since the class name is from the list of the class files under the classloader.
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }

  /**
   * Extracts and returns name of the plugin.
   */
  private String getPluginName(Class<?> cls) {
    Name annotation = cls.getAnnotation(Name.class);
    return annotation == null || annotation.value().isEmpty() ? cls.getName() : annotation.value();
  }

  /**
   * Get all the {@link io.cdap.cdap.api.annotation.Requirements} specified by a plugin as {@link Requirements}.
   * The requirements are case insensitive and always represented in lowercase.
   *
   * @param cls the plugin class whose requirement needs to be found
   *
   * @return {@link Requirements} containing the requirements specified by the plugin (in lowercase). If the plugin does
   * not specify any {@link io.cdap.cdap.api.annotation.Requirements} then the {@link Requirements} will be empty.
   */
  @VisibleForTesting
  Requirements getArtifactRequirements(Class<?> cls) {
    io.cdap.cdap.api.annotation.Requirements annotation =
      cls.getAnnotation(io.cdap.cdap.api.annotation.Requirements.class);
    if (annotation == null) {
      return Requirements.EMPTY;
    }
    return new Requirements(getAnnotationValues(annotation.datasetTypes()),
                            getAnnotationValues(annotation.capabilities()));
  }

  private Set<String> getAnnotationValues(String[] field) {
    return Arrays.stream(field).map(String::trim).map(String::toLowerCase).filter(Objects::nonNull)
      .filter(s -> !s.isEmpty()).collect(Collectors.toSet());
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
        int modifiers = field.getModifiers();
        if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers) || field.isSynthetic()) {
          continue;
        }

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

    Macro macroAnnotation = field.getAnnotation(Macro.class);
    boolean macroSupported = macroAnnotation != null;
    if (rawType.isPrimitive()) {
      return new PluginPropertyField(name, description, rawType.getName(), true, macroSupported);
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

    return new PluginPropertyField(name, description, rawType.getSimpleName().toLowerCase(), required, macroSupported);
  }

  /**
   * Detects if a class is annotated with {@link Plugin} without loading the class.
   *
   * @param className name of the class
   * @param classLoader ClassLoader for loading the class file of the given class
   * @return true if the given class is annotated with {@link Plugin}
   */
  private boolean isPlugin(String className, ClassLoader classLoader) {
    try (InputStream is = classLoader.getResourceAsStream(className.replace('.', '/') + ".class")) {
      if (is == null) {
        return false;
      }

      // Use ASM to inspect the class bytecode to see if it is annotated with @Plugin
      final boolean[] isPlugin = new boolean[1];
      ClassReader cr = new ClassReader(is);
      cr.accept(new ClassVisitor(Opcodes.ASM5) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          if (Plugin.class.getName().equals(Type.getType(desc).getClassName()) && visible) {
            isPlugin[0] = true;
          }
          return super.visitAnnotation(desc, visible);
        }
      }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);

      return isPlugin[0];
    } catch (IOException e) {
      // If failed to open the class file, then it cannot be a plugin
      LOG.warn("Failed to open class file for {}", className, e);
      return false;
    }
  }
}
