/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.LocalizationUtils;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.ForwardingLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

/**
 * A utility class to help determine Spark supports and locating Spark jar.
 * This class shouldn't use any classes from Spark/Scala.
 */
public final class SparkPackageUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPackageUtils.class);

  // The prefix for spark environment variable names. It is being setup by the startup script.
  private static final String SPARK_ENV_PREFIX = "_SPARK_";
  // Environment variable name for the spark conf directory.
  private static final String SPARK_CONF_DIR = "SPARK_CONF_DIR";
  // Environment variable name for the spark version. It is being setup by the startup script.
  private static final String SPARK_VERSION = "SPARK_VERSION";
  // Environment variable name for the location to the spark framework direction in local file system.
  // It is setup for the YARN container to find the spark framework in the container directory.
  private static final String SPARK_LIBRARY = "SPARK_LIBRARY";
  // File name for the spark default config
  private static final String SPARK_DEFAULTS_CONF = "spark-defaults.conf";
  // Spark1 conf key for spark-assembly jar location
  private static final String SPARK_YARN_JAR = "spark.yarn.jar";
  // Spark2 conf key for spark archive (zip of jars) location
  private static final String SPARK_YARN_ARCHIVE = "spark.yarn.archive";

  // Environment variable name for locating spark home directory
  private static final String SPARK_HOME = Constants.SPARK_HOME;

  // File name of the Spark conf directory as defined by the Spark framework
  // This is for the Hack to workaround CDAP-5019 (SPARK-13441)
  public static final String LOCALIZED_CONF_DIR = "__spark_conf__";

  // The spark framework on the local file system.
  private static final EnumMap<SparkCompat, Set<File>> LOCAL_SPARK_LIBRARIES = new EnumMap<>(SparkCompat.class);

  // The spark framework ready to be localized
  private static final EnumMap<SparkCompat, SparkFramework> SPARK_FRAMEWORKS = new EnumMap<>(SparkCompat.class);

  private static Map<String, String> sparkEnv;

  /**
   * Returns the set of jar files for the spark library.
   */
  public static synchronized Set<File> getLocalSparkLibrary(SparkCompat sparkCompat) {
    Set<File> jars = LOCAL_SPARK_LIBRARIES.get(sparkCompat);
    if (jars != null) {
      return jars;
    }

    // This is for YARN container, which the launcher will set the environment variable
    // See #prepareSparkResources
    String sparkLibrary = System.getenv(SPARK_LIBRARY);

    // In future, we could have SPARK1 and SPARK2 home.
    String sparkHome = System.getenv(SPARK_HOME);
    if (sparkLibrary == null && sparkHome == null) {
      throw new IllegalStateException("Spark not found. Please set environment variable " + SPARK_HOME);
    }

    switch (sparkCompat) {
      case SPARK1_2_10:
        LOCAL_SPARK_LIBRARIES.put(sparkCompat, Collections.singleton(getSpark1AssemblyJar(sparkLibrary, sparkHome)));
        break;
      case SPARK2_2_11:
        LOCAL_SPARK_LIBRARIES.put(sparkCompat, getSpark2LibraryJars(sparkLibrary, sparkHome));
        break;
      default:
        // This shouldn't happen
        throw new IllegalStateException("Unsupported Spark version " + sparkCompat);
    }

    return LOCAL_SPARK_LIBRARIES.get(sparkCompat);
  }

  /**
   * Prepares the resources that need to be localized to the Spark client container.
   *
   * @param sparkCompat the spark version to prepare for
   * @param locationFactory the location factory for uploading files
   * @param tempDir a temporary directory for file creation
   * @param localizeResources A map from localized name to {@link LocalizeResource} for this method to update
   * @param env the environment map to update
   * @throws IOException if failed to prepare the spark resources
   */
  public static void prepareSparkResources(SparkCompat sparkCompat, LocationFactory locationFactory, File tempDir,
                                           Map<String, LocalizeResource> localizeResources,
                                           Map<String, String> env) throws IOException {
    Properties sparkConf = getSparkDefaultConf();

    // Localize the spark framework
    SparkFramework framework = prepareSparkFramework(sparkCompat, locationFactory, tempDir);
    framework.addLocalizeResource(localizeResources);
    framework.updateSparkConf(sparkConf);
    framework.updateSparkEnv(env);

    // Localize the spark-defaults.conf file
    File sparkDefaultConfFile = saveSparkDefaultConf(sparkConf,
                                                     File.createTempFile(SPARK_DEFAULTS_CONF, null, tempDir));
    localizeResources.put(SPARK_DEFAULTS_CONF, new LocalizeResource(sparkDefaultConfFile));

    // Shallow copy all files under directory defined by $HADOOP_CONF_DIR
    // If $HADOOP_CONF_DIR is not defined, use the location of "yarn-site.xml" to determine the directory
    // This is part of workaround for CDAP-5019 (SPARK-13441).
    File hadoopConfDir = null;
    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())) {
      hadoopConfDir = new File(System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.key()));
    } else {
      URL yarnSiteLocation = SparkPackageUtils.class.getClassLoader().getResource("yarn-site.xml");
      if (yarnSiteLocation != null) {
        try {
          hadoopConfDir = new File(yarnSiteLocation.toURI()).getParentFile();
        } catch (URISyntaxException e) {
          // Shouldn't happen
          LOG.warn("Failed to derive HADOOP_CONF_DIR from yarn-site.xml");
        }
      }
    }
    if (hadoopConfDir != null && hadoopConfDir.isDirectory()) {
      try {
        final File targetFile = File.createTempFile(LOCALIZED_CONF_DIR, ".zip", tempDir);
        try (
          ZipOutputStream zipOutput = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))
        ) {
          for (File file : DirUtils.listFiles(hadoopConfDir)) {
            // Shallow copy of files under the hadoop conf dir. Ignore files that cannot be read
            if (file.isFile() && file.canRead()) {
              zipOutput.putNextEntry(new ZipEntry(file.getName()));
              Files.copy(file.toPath(), zipOutput);
            }
          }
        }
        localizeResources.put(LOCALIZED_CONF_DIR, new LocalizeResource(targetFile, true));
      } catch (IOException e) {
        LOG.warn("Failed to create archive from {}", hadoopConfDir, e);
      }
    }
  }

  /**
   * Locates the spark-assembly jar from the local file system for Spark1.
   *
   * @param sparkLibrary file path to the spark-assembly jar in the local file system
   * @param sparkHome file path to the spark home.
   *
   * @return the spark-assembly jar location
   * @throws IllegalStateException if cannot locate the spark assembly jar
   */
  private static File getSpark1AssemblyJar(@Nullable String sparkLibrary, String sparkHome) {
    if (sparkLibrary != null) {
      return new File(sparkLibrary);
    }

    // Look for spark-assembly.jar symlink
    Path assemblyJar = Paths.get(sparkHome, "lib", "spark-assembly.jar");
    if (Files.isSymbolicLink(assemblyJar)) {
      return assemblyJar.toFile();
    }

    // No symbolic link exists. Search for spark-assembly*.jar in the lib directory
    final List<Path> jar = new ArrayList<>(1);
    Path sparkLib = Paths.get(sparkHome, "lib");
    final PathMatcher pathMatcher = sparkLib.getFileSystem().getPathMatcher("glob:spark-assembly*.jar");
    try {
      Files.walkFileTree(sparkLib, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          // Take the first file match
          if (attrs.isRegularFile() && pathMatcher.matches(file.getFileName())) {
            jar.add(file);
            return FileVisitResult.TERMINATE;
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
          // Ignore error
          return FileVisitResult.CONTINUE;
        }

      });

    } catch (IOException e) {
      // Just log, don't throw.
      // If we already located the Spark Assembly jar during visiting, we can still use the jar.
      LOG.warn("Exception raised while inspecting {}", sparkLib, e);
    }

    Preconditions.checkState(!jar.isEmpty(), "Failed to locate Spark library from %s", sparkHome);

    assemblyJar = jar.get(0);
    LOG.debug("Located Spark Assembly JAR in {}", assemblyJar);
    return assemblyJar.toFile();
  }

  /**
   * Finds the set of jar files for the Spark libarary and its dependencies in local file system for Spark2.
   *
   * @param sparkLibrary file path to the spark-assembly jar in the local file system
   * @param sparkHome file path to the spark home.
   * @return A set of jar files
   * @throws IllegalStateException if no jar file is found
   */
  private static Set<File> getSpark2LibraryJars(@Nullable String sparkLibrary, String sparkHome) {
    // There should be a jars directory under SPARK_HOME.
    File jarsDir = sparkLibrary == null ? new File(sparkHome, "jars") : new File(sparkLibrary);
    Preconditions.checkState(jarsDir.isDirectory(), "Expected %s to be a directory for Spark2", jarsDir);

    Set<File> jars = new HashSet<>(DirUtils.listFiles(jarsDir, "jar"));
    Preconditions.checkState(!jars.isEmpty(), "No jar files found in %s for Spark2", jarsDir);
    LOG.debug("Located Spark library in in {}", jarsDir);
    return jars;
  }

  /**
   * Returns the Spark environment setup via the start up script.
   */
  public static synchronized Map<String, String> getSparkEnv() {
    if (sparkEnv != null) {
      return sparkEnv;
    }

    Map<String, String> env = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      if (entry.getKey().startsWith(SPARK_ENV_PREFIX)) {
        env.put(entry.getKey().substring(SPARK_ENV_PREFIX.length()), entry.getValue());
      }
    }

    // Spark using YARN and it is needed for both Workflow and Spark runner. We need to set it
    // because inside Spark code, it will set and unset the SPARK_YARN_MODE system properties, causing
    // fork in distributed mode not working. Setting it in the environment, which Spark uses for defaults,
    // so it can't be unset by Spark
    env.put("SPARK_YARN_MODE", "true");

    sparkEnv = Collections.unmodifiableMap(env);
    return sparkEnv;
  }

  /**
   * Returns the environment for the Spark client container.
   */
  public static Map<String, String> getSparkClientEnv() {
    Map<String, String> env = new LinkedHashMap<>(getSparkEnv());

    // The spark-defaults.conf will be localized to container
    // and we shouldn't have SPARK_HOME set
    env.put(SPARK_CONF_DIR, "$PWD");
    env.remove(SPARK_HOME);

    // Spark using YARN and it is needed for both Workflow and Spark runner. We need to set it
    // because inside Spark code, it will set and unset the SPARK_YARN_MODE system properties, causing
    // fork in distributed mode not working. Setting it in the environment, which Spark uses for defaults,
    // so it can't be unset by Spark
    env.put("SPARK_YARN_MODE", "true");

    return Collections.unmodifiableMap(env);
  }

  /**
   * Loads the spark-defaults.conf based on the environment.
   *
   * @return a {@link Properties} object representing Spark default configurations.
   */
  public static synchronized Properties getSparkDefaultConf() {
    Properties properties = new Properties();

    File confFile = SparkPackageUtils.locateSparkDefaultsConfFile(getSparkEnv());
    if (confFile == null) {
      return properties;
    }
    try (Reader reader = com.google.common.io.Files.newReader(confFile, StandardCharsets.UTF_8)) {
      properties.load(reader);
    } catch (IOException e) {
      LOG.warn("Failed to load Spark default configurations from {}.", confFile, e);
    }
    return properties;
  }

  /**
   * Locates the spark-defaults.conf file location.
   */
  @Nullable
  private static File locateSparkDefaultsConfFile(Map<String, String> env) {
    File confFile = null;
    if (env.containsKey(SPARK_CONF_DIR)) {
      // If SPARK_CONF_DIR is defined, then the default conf should be under it
      confFile = new File(env.get(SPARK_CONF_DIR), SPARK_DEFAULTS_CONF);
    } else if (env.containsKey(SPARK_HOME)) {
      // Otherwise, it should be under SPARK_HOME/conf
      confFile = new File(new File(env.get(SPARK_HOME), "conf"), SPARK_DEFAULTS_CONF);
    }

    return confFile == null || !confFile.isFile() ? null : confFile;
  }

  /**
   * Saves the spark config into the given file
   */
  private static File saveSparkDefaultConf(Properties sparkConf, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      sparkConf.store(writer, null);
    }
    return file;
  }

  /**
   * Prepares the spark framework and have it ready on a location.
   *
   * @param cConf the configuration
   * @param locationFactory the {@link LocationFactory} for storing the spark framework jar(s).
   * @return a {@link LocalizeResource} containing information for the spark framework for file localization
   */
  private static synchronized SparkFramework prepareSparkFramework(SparkCompat sparkCompat, LocationFactory lf,
                                                                   File tempDir) throws IOException {

    SparkFramework framework = SPARK_FRAMEWORKS.get(sparkCompat);
    if (framework != null && framework.getFrameworkLocation(lf).exists()) {
      return framework;
    }

    Properties sparkConf = getSparkDefaultConf();
    switch (sparkCompat) {
      case SPARK1_2_10:
        SPARK_FRAMEWORKS.put(sparkCompat, prepareSpark1Framework(sparkConf, lf));
      break;

      case SPARK2_2_11:
        SPARK_FRAMEWORKS.put(sparkCompat, prepareSpark2Framework(sparkConf, lf, tempDir));
      break;

      default:
        throw new IllegalArgumentException("Unsupported spark version " + sparkCompat);
    }

    return SPARK_FRAMEWORKS.get(sparkCompat);
  }

  /**
   * Prepares the Spark 1 framework on the location
   *
   * @param sparkConf the spark configuration
   * @param locationFactory the {@link LocationFactory} for saving the spark framework jar
   * @return A {@link SparkFramework} containing information about the spark framework in localization context.
   * @throws IOException If failed to prepare the framework.
   */
  private static SparkFramework prepareSpark1Framework(Properties sparkConf,
                                                       LocationFactory locationFactory) throws IOException {
    String sparkYarnJar = sparkConf.getProperty(SPARK_YARN_JAR);

    if (sparkYarnJar != null) {
      Location frameworkLocation = locationFactory.create(URI.create(sparkYarnJar));
      if (frameworkLocation.exists()) {
        return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), false), SPARK_YARN_JAR);
      }
      LOG.warn("The location {} set by '{}' does not exist.", frameworkLocation, SPARK_YARN_JAR);
    }

    // If spark.yarn.jar is not defined or doesn't exists, get the spark-assembly jar from local FS and upload it
    File sparkAssemblyJar = Iterables.getFirst(getLocalSparkLibrary(SparkCompat.SPARK1_2_10), null);
    Location frameworkDir = locationFactory.create("/framework/spark");
    Location frameworkLocation = frameworkDir.append(sparkAssemblyJar.getName());

    // Upload assembly jar to the framework location if not exists
    if (!frameworkLocation.exists()) {
      frameworkDir.mkdirs("755");

      try (OutputStream os = frameworkLocation.getOutputStream("644")) {
        Files.copy(sparkAssemblyJar.toPath(), os);
      }
    }
    return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), false), SPARK_YARN_JAR);
  }

  /**
   * Prepares the Spark 2 framework on the location.
   *
   * @param sparkConf the spark configuration
   * @param locationFactory the {@link LocationFactory} for saving the spark framework jar
   * @param tempDir directory for temporary file creation
   * @return A {@link SparkFramework} containing information about the spark framework in localization context.
   * @throws IOException If failed to prepare the framework.
   */
  private static SparkFramework prepareSpark2Framework(Properties sparkConf, LocationFactory locationFactory,
                                                       File tempDir) throws IOException {
    String sparkYarnArchive = sparkConf.getProperty(SPARK_YARN_ARCHIVE);

    if (sparkYarnArchive != null) {
      Location frameworkLocation = locationFactory.create(URI.create(sparkYarnArchive));
      if (frameworkLocation.exists()) {
        return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), true), SPARK_YARN_ARCHIVE);
      }
      LOG.warn("The location {} set by '{}' does not exist.", frameworkLocation, SPARK_YARN_ARCHIVE);
    }

    // If spark.yarn.archive is not defined or doesn't exists, build a archive zip from local FS and upload it
    String sparkVersion = System.getenv(SPARK_VERSION);
    sparkVersion = sparkVersion == null ? SparkCompat.SPARK2_2_11.getCompat() : sparkVersion;

    String archiveName = "spark.archive-" + sparkVersion + ".zip";
    Location frameworkDir = locationFactory.create("/framework/spark");
    Location frameworkLocation = frameworkDir.append(archiveName);

    if (!frameworkLocation.exists()) {
      File archive = new File(tempDir, archiveName);
      try {
        try (ZipOutputStream zipOutput = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(archive)))) {
          zipOutput.setLevel(Deflater.NO_COMPRESSION);
          for (File file : getLocalSparkLibrary(SparkCompat.SPARK2_2_11)) {
            zipOutput.putNextEntry(new ZipEntry(file.getName()));
            Files.copy(file.toPath(), zipOutput);
            zipOutput.closeEntry();
          }
        }

        // Upload spark archive to the framework location
        frameworkDir.mkdirs("755");

        try (OutputStream os = frameworkLocation.getOutputStream("644")) {
          Files.copy(archive.toPath(), os);
        }
      } finally {
        archive.delete();
      }
    }
    return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), true), SPARK_YARN_ARCHIVE);
  }

  /**
   * Resolves a {@link URI} representation from the given {@link Location}. It resolves the URI in the same way
   * as Spark does.
   */
  private static URI resolveURI(Location location) throws IOException {
    LocationFactory locationFactory = location.getLocationFactory();

    while (locationFactory instanceof ForwardingLocationFactory) {
      locationFactory = ((ForwardingLocationFactory) locationFactory).getDelegate();
    }
    if (!(locationFactory instanceof FileContextLocationFactory)) {
      return location.toURI();
    }

    // Resolves the URI the way as Spark does
    Configuration hConf = ((FileContextLocationFactory) locationFactory).getConfiguration();
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location.toURI().getPath());
    path = path.getFileSystem(hConf).makeQualified(path);
    return ((FileContextLocationFactory) locationFactory).getFileContext().resolvePath(path).toUri();
  }

  private SparkPackageUtils() {
  }

  /**
   * Class to representing spark framework resource
   */
  private static final class SparkFramework {
    private final LocalizeResource localizeResource;
    private final String configKey;

    SparkFramework(LocalizeResource localizeResource, String configKey) {
      this.localizeResource = localizeResource;
      this.configKey = configKey;
    }

    Location getFrameworkLocation(LocationFactory lf) {
      return lf.create(localizeResource.getURI());
    }

    void addLocalizeResource(Map<String, LocalizeResource> resources) {
      resources.put(LocalizationUtils.getLocalizedName(localizeResource.getURI()), localizeResource);
    }

    void updateSparkConf(Properties sparkConf) {
      sparkConf.setProperty(configKey, localizeResource.getURI().toString());
    }

    void updateSparkEnv(Map<String, String> env) {
      env.put(SPARK_LIBRARY, LocalizationUtils.getLocalizedName(localizeResource.getURI()));
    }
  }
}
