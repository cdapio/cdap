/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.proto.id.NamespaceId;
import java.lang.reflect.Type;

/**
 * Util class that contains helper methods related to handling of artifacts.
 */
public final class Artifacts {

  public static String getFileName(ArtifactId artifactId) {
    return String.format("%s-%s-%s.jar", artifactId.getScope(), artifactId.getName(),
        artifactId.getVersion());
  }

  /**
   * Resolve the application's config type.
   *
   * @param appClass the application class to resolve the config type for
   * @return the resolved config type
   * @throws IllegalArgumentException if the config type is not a valid type
   */
  public static Type getConfigType(Class<?> appClass) {
    // Class has to be a child of Application
    Preconditions.checkArgument(Application.class.isAssignableFrom(appClass), "The given class : " + appClass
      + " is not supported. Type must be a child class of Application");
    TypeToken<?> configType = TypeToken.of(appClass)
      .resolveType(Application.class.getTypeParameters()[0]);
    if (Reflections.isResolved(configType.getType())) {
      // Default the type to Config.class if the resolved type is not subclass of Config.
      // It normally won't happen, unless someone generate the bytecode directly.
      // This is for Scala. If the user don't specify any type parameter, Scala will automatically insert
      // "scala.runtime.Nothing$" as the type parameter.
      if (Config.class.isAssignableFrom(configType.getRawType())) {
        return configType.getType();
      }
      return Config.class;
    }

    // It has to be Config
    Preconditions.checkArgument(Config.class == configType.getRawType(),
        "Application config type " + configType + " not supported. "
            + "Type must extend Config and cannot be parameterized.");
    return Config.class;
  }

  /**
   * Converts a {@link ArtifactId} to {@link io.cdap.cdap.proto.id.ArtifactId}.
   *
   * @param namespaceId the user namespace to use
   * @param artifactId the artifact id to convert
   */
  public static io.cdap.cdap.proto.id.ArtifactId toProtoArtifactId(NamespaceId namespaceId,
      ArtifactId artifactId) {
    ArtifactScope scope = artifactId.getScope();
    NamespaceId artifactNamespace =
        scope == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : namespaceId;
    return artifactNamespace.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
  }

  /**
   * Loads and instantiates an {@link Application} class without running its static initializer
   * ({@code <clinit>}) until it is verified to implement {@link Application}.
   *
   * @param classLoader classloader to load the application class from
   * @param className fully qualified name of the application class
   * @return an instantiated {@link Application}
   * @throws ClassNotFoundException if the application class cannot be found
   * @throws InstantiationException if the application class cannot be instantiated
   * @throws IllegalAccessException if the application class or its no-arg constructor is not
   *     accessible
   * @throws IllegalStateException if the loaded class does not implement {@link Application}
   */
  public static Application<?> instantiateApplication(ClassLoader classLoader, String className)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> appClass = classLoader.loadClass(className);
    if (!Application.class.isAssignableFrom(appClass)) {
      throw new IllegalStateException(
          String.format("Application main class is of invalid type: %s", appClass.getName()));
    }
    return appClass.asSubclass(Application.class).newInstance();
  }

  /**
   * Translates missing dependency errors ({@link ClassNotFoundException} or
   * {@link NoClassDefFoundError}) in the root cause of {@code t} into an
   * {@link InvalidArtifactException}, or an {@link IllegalStateException} when Spark is not
   * available in the platform.
   *
   * @param t throwable to inspect
   * @param artifactClassLoader classloader used to probe Spark availability
   * @throws InvalidArtifactException if the root cause is a missing dependency class or failed
   *     class initialization
   * @throws IllegalStateException if a missing Spark class is caused by Spark unavailability
   */
  public static void throwIfMissingDependency(Throwable t, ClassLoader artifactClassLoader)
      throws InvalidArtifactException {
    Throwable rootCause = Throwables.getRootCause(t);
    if (!(rootCause instanceof ClassNotFoundException
        || rootCause instanceof NoClassDefFoundError)) {
      return;
    }

    String missingClass = rootCause.getMessage();
    if (rootCause instanceof NoClassDefFoundError
        && missingClass != null
        && missingClass.startsWith("Could not initialize class ")) {
      throw new InvalidArtifactException(
          "Failed to initialize class: " + missingClass, t);
    }

    if (missingClass != null) {
      // NoClassDefFoundError reports internal slash-separated names (e.g. org/apache/spark/...).
      String normalizedClass = missingClass.replace('/', '.');
      if (normalizedClass.startsWith("org.apache.spark.")
          || normalizedClass.startsWith("io.cdap.cdap.api.spark.")) {
        // Try to load SparkContext, which should be available if Spark is available in the platform
        try {
          artifactClassLoader.loadClass("org.apache.spark.SparkContext");
        } catch (ClassNotFoundException e) {
          // Spark is not available, it is most likely caused by missing Spark in the platform
          throw new IllegalStateException(
              "Missing Spark related class " + missingClass
                  + ". It may be caused by unavailability of Spark. "
                  + "Please verify environment variable " + Constants.SPARK_HOME
                  + " is set correctly", t);
        }

        // Spark is available, can be caused by incompatible Spark version
        throw new InvalidArtifactException(
            "Missing Spark related class " + missingClass
                + ". Configured to use Spark located at " + System.getenv(Constants.SPARK_HOME)
                + ", which may be incompatible with the one required by the application", t);
      }
    }

    // If Spark is available or the missing class is not a spark related class,
    // then the missing class is most likely due to some missing library in the artifact jar
    String target = (missingClass != null && !missingClass.isEmpty())
        ? "class " + missingClass
        : "required class";
    throw new InvalidArtifactException(
        "Missing " + target
            + ". It may be caused by missing dependency jar(s) in the artifact jar.", t);
  }

  private Artifacts() {
    // Prevent instantiation of util class.
  }
}
