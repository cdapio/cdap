/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.LocalizationUtils;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.runtime.spi.SparkCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.ForwardingLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

/**
 * A utility class to help determine Spark supports and locating Spark jars and PySpark libraries.
 * This class shouldn't use any classes from Spark/Scala.
 */
public final class SparkPackageUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPackageUtils.class);

  // Environment variable name to instruct Spark submit to run in YARN mode
  public static final String SPARK_YARN_MODE = "SPARK_YARN_MODE";
  // The prefix for spark environment variable names. It is being setup by the startup script.
  private static final String SPARK_ENV_PREFIX = "_SPARK_";
  // Environment variable name for the spark conf directory.
  private static final String SPARK_CONF_DIR = "SPARK_CONF_DIR";
  // Environment variable name for the spark version. It is being setup by the startup script.
  private static final String SPARK_VERSION = "SPARK_VERSION";
  // Environment variable name for the location to the spark framework direction in local file system.
  // It is setup for the YARN container to find the spark framework in the container directory.
  private static final String SPARK_LIBRARY = "SPARK_LIBRARY";
  // Environment variable name for a comma separated list of local file paths that contains
  // the pyspark.zip and py4j-*.zip
  private static final String PYSPARK_ARCHIVES_PATH = "PYSPARK_ARCHIVES_PATH";
  // File name for the spark default config
  private static final String SPARK_DEFAULTS_CONF = "spark-defaults.conf";
  // File name for the spark environment script
  private static final String SPARK_ENV_SH = "spark-env.sh";
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

  // The pyspark archive files on the local file system.
  private static final EnumMap<SparkCompat, Set<File>> LOCAL_PY_SPARK_ARCHIVES = new EnumMap<>(SparkCompat.class);

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
      // If both SPARK_LIBRARY and SPARK_HOME are not set, it should be in standalone mode, in which
      // Spark classes are in the classloader of this class, which is loaded via the program runtime provider extension.
      // This class shouldn't have dependency on Spark itself, hence using the resource name
      URL sparkContextURL = SparkPackageUtils.class.getClassLoader().getResource("org/apache/spark/SparkContext.class");
      if (sparkContextURL == null) {
        // This error message is for user to read. Usage of SPARK_LIBRARY / classloader is internal, hence not showing.
        throw new IllegalStateException("Spark not found. Please set environment variable " + SPARK_HOME);
      }

      URL sparkURL = ClassLoaders.getClassPathURL("org.apache.spark.SparkContext", sparkContextURL);
      LOCAL_SPARK_LIBRARIES.put(sparkCompat, Collections.singleton(new File(sparkURL.getPath())));
    } else {
      LOCAL_SPARK_LIBRARIES.put(sparkCompat, getSparkLibraryJars(sparkLibrary, sparkHome));
    }

    return LOCAL_SPARK_LIBRARIES.get(sparkCompat);
  }

  /**
   * Returns the set of PySpark archive files.
   */
  public static synchronized Set<File> getLocalPySparkArchives(SparkCompat sparkCompat) {
    Set<File> archives = LOCAL_PY_SPARK_ARCHIVES.get(sparkCompat);
    if (archives != null) {
      return archives;
    }

    archives = new LinkedHashSet<>();
    String archivesPath = System.getenv(PYSPARK_ARCHIVES_PATH);
    String sparkHome = System.getenv(SPARK_HOME);

    if (sparkHome == null && archivesPath == null) {
      LOG.warn("Failed to determine location of PySpark libraries. Running PySpark program might fail. " +
                 "Please set environment variable {} to make PySpark available.", SPARK_HOME);
    } else {
      if (archivesPath != null) {
        // If the archives path is explicitly set, use it
        for (String path : archivesPath.split(",")) {
          archives.add(new File(path).getAbsoluteFile());
        }
      } else {
        // Otherwise, grab all .zip files under $SPARK_HOME/python/lib/*.zip
        archives.addAll(DirUtils.listFiles(Paths.get(sparkHome, "python", "lib").toFile(), "zip"));
      }
    }

    archives = Collections.unmodifiableSet(archives);
    LOCAL_PY_SPARK_ARCHIVES.put(sparkCompat, archives);
    return archives;
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

    // Localize PySpark.
    List<String> pySparkArchives = new ArrayList<>();
    for (File archive : getLocalPySparkArchives(sparkCompat)) {
      localizeResources.put(archive.getName(), new LocalizeResource(archive));
      pySparkArchives.add(archive.getName());
    }
    // Set the PYSPARK_ARCHIVES_PATH environment variable in the YARN container.
    env.put(PYSPARK_ARCHIVES_PATH, Joiner.on(",").join(pySparkArchives));


    // Localize the spark-defaults.conf file
    File sparkDefaultConfFile = saveSparkDefaultConf(sparkConf,
                                                     File.createTempFile(SPARK_DEFAULTS_CONF, null, tempDir));
    localizeResources.put(SPARK_DEFAULTS_CONF, new LocalizeResource(sparkDefaultConfFile));
    env.putAll(getSparkClientEnv());

    // Shallow copy all files under directory defined by $HADOOP_CONF_DIR
    // If $HADOOP_CONF_DIR is not defined, use the location of "yarn-site.xml" to determine the directory
    // This is part of workaround for CDAP-5019 (SPARK-13441) and CDAP-12330
    List<File> configDirs = new ArrayList<>();

    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())) {
      configDirs.add(new File(System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())));
    } else {
      URL yarnSiteLocation = SparkPackageUtils.class.getClassLoader().getResource("yarn-site.xml");
      if (yarnSiteLocation == null || !"file".equals(yarnSiteLocation.getProtocol())) {
        LOG.warn("Failed to derive HADOOP_CONF_DIR from yarn-site.xml location: {}", yarnSiteLocation);
      } else {
        configDirs.add(new File(yarnSiteLocation.getPath()).getParentFile());
      }
    }

    Splitter splitter = Splitter.on(File.pathSeparatorChar).omitEmptyStrings();

    if (!configDirs.isEmpty()) {
      File targetFile = File.createTempFile(LOCALIZED_CONF_DIR, ".zip", tempDir);
      Set<String> entries = new HashSet<>();
      try (ZipOutputStream output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))) {
        for (File configDir : configDirs) {
          try {
            LOG.debug("Adding files from {} to {}.zip", configDir, LOCALIZED_CONF_DIR);
            addConfigFiles(configDir, entries, output);
          } catch (IOException e) {
            LOG.warn("Failed to create archive from {}", configDir, e);
          }
        }
      }
      localizeResources.put(LOCALIZED_CONF_DIR, new LocalizeResource(targetFile, true));
      env.put("YARN_CONF_DIR", "$PWD/" + LOCALIZED_CONF_DIR);
    }
  }

  /**
   * Shallow copy of files under the given directory. Files that cannot be read are ignored.
   *
   * @param dir the directory
   * @param entries entries that were already added. This method should update this set when adding new entries
   * @param zipOutput the {@link ZipOutputStream} to write to
   */
  private static void addConfigFiles(File dir, Set<String> entries, ZipOutputStream zipOutput) throws IOException {
    for (File file : DirUtils.listFiles(dir)) {
      // Ignore files that cannot be read or the same file was added (happen when this method get called multiple times)
      if (!file.isFile() || !file.canRead() || !entries.add(file.getName())) {
        continue;
      }

      zipOutput.putNextEntry(new ZipEntry(file.getName()));

      // Rewrite the hive-site.xml file to set the Hive Metastore delegation token signature
      if ("hive-site.xml".equals(file.getName())) {
        Configuration conf = new Configuration(false);
        conf.clear();
        conf.addResource(file.toURI().toURL());
        conf.writeXml(zipOutput);
      } else {
        Files.copy(file.toPath(), zipOutput);
      }

      zipOutput.closeEntry();
    }
  }

  /**
   * Finds the set of jar files for the Spark libarary and its dependencies in local file system.
   *
   * @param sparkLibrary file path to the spark-assembly jar in the local file system
   * @param sparkHome file path to the spark home.
   * @return A set of jar files
   * @throws IllegalStateException if no jar file is found
   */
  private static Set<File> getSparkLibraryJars(@Nullable String sparkLibrary, String sparkHome) {
    // There should be a jars directory under SPARK_HOME.
    File jarsDir = sparkLibrary == null ? new File(sparkHome, "jars") : new File(sparkLibrary);
    Preconditions.checkState(jarsDir.isDirectory(), "Expected %s to be a directory for Spark", jarsDir);

    Set<File> jars = new HashSet<>(DirUtils.listFiles(jarsDir, "jar"));
    Preconditions.checkState(!jars.isEmpty(), "No jar files found in %s for Spark", jarsDir);
    LOG.debug("Located Spark library in in {}", jarsDir);
    return jars;
  }

  /**
   * Returns the Spark environment setup via the start up script.
   */
  private static synchronized Map<String, String> getSparkEnv() {
    if (sparkEnv != null) {
      return sparkEnv;
    }

    Map<String, String> env = new LinkedHashMap<>(loadSparkEnv(System.getenv(SPARK_HOME)));
    env.putAll(System.getenv());

    // Overwrite the system environments with the one set up by the startup script in functions.sh
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      if (entry.getKey().startsWith(SPARK_ENV_PREFIX)) {
        env.put(entry.getKey().substring(SPARK_ENV_PREFIX.length()), entry.getValue());
      }
    }

    // Spark using YARN and it is needed for both Workflow and Spark runner. We need to set it
    // because inside Spark code, it will set and unset the SPARK_YARN_MODE system properties, causing
    // fork in distributed mode not working. Setting it in the environment, which Spark uses for defaults,
    // so it can't be unset by Spark
    env.put(SPARK_YARN_MODE, "true");

    sparkEnv = Collections.unmodifiableMap(env);
    return sparkEnv;
  }

  /**
   * Returns the environment for the Spark client container.
   */
  private static Map<String, String> getSparkClientEnv() {
    Map<String, String> env = new LinkedHashMap<>(getSparkEnv());

    // The spark-defaults.conf will be localized to container
    // and we shouldn't have SPARK_HOME set
    env.put(SPARK_CONF_DIR, "$PWD");
    env.remove(SPARK_HOME);

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

    // Remove "spark.master" and "spark.submit.deployMode"
    properties.remove("spark.master");
    properties.remove("spark.submit.deployMode");
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
    SPARK_FRAMEWORKS.put(sparkCompat, prepareSparkFramework(sparkCompat, sparkConf, lf, tempDir));

    return SPARK_FRAMEWORKS.get(sparkCompat);
  }

  /**
   * Prepares the Spark framework on the location.
   *
   * @param sparkConf the spark configuration
   * @param locationFactory the {@link LocationFactory} for saving the spark framework jar
   * @param tempDir directory for temporary file creation
   * @return A {@link SparkFramework} containing information about the spark framework in localization context.
   * @throws IOException If failed to prepare the framework.
   */
  private static SparkFramework prepareSparkFramework(SparkCompat sparkCompat,
                                                      Properties sparkConf,
                                                      LocationFactory locationFactory,
                                                      File tempDir) throws IOException {
    String sparkYarnArchive = sparkConf.getProperty(SPARK_YARN_ARCHIVE);

    if (sparkYarnArchive != null) {
      URI sparkYarnArchiveURI = URI.create(sparkYarnArchive);
      if (locationFactory.getHomeLocation().toURI().getScheme().equals(sparkYarnArchiveURI.getScheme())) {
        Location frameworkLocation = locationFactory.create(URI.create(sparkYarnArchive));
        if (frameworkLocation.exists()) {
          return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), true), SPARK_YARN_ARCHIVE);
        }
        LOG.warn("The location {} set by '{}' does not exist.", frameworkLocation, SPARK_YARN_ARCHIVE);
      }
    }

    // If spark.yarn.archive is not defined or doesn't exists, build a archive zip from local FS and upload it
    String sparkVersion = System.getenv(SPARK_VERSION);
    sparkVersion = sparkVersion == null ? sparkCompat.getCompat() : sparkVersion;

    String archiveName = "spark.archive-" + sparkVersion + "-" + VersionInfo.getVersion() + ".zip";
    Location frameworkDir = locationFactory.create("/framework/spark");
    Location frameworkLocation = frameworkDir.append(archiveName);
    File archive = new File(tempDir, archiveName);
    try {
      try (
        ZipOutputStream zipOutput = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(archive)))) {
        zipOutput.setLevel(Deflater.NO_COMPRESSION);
        for (File file : getLocalSparkLibrary(sparkCompat)) {
          zipOutput.putNextEntry(new ZipEntry(file.getName()));
          Files.copy(file.toPath(), zipOutput);
          zipOutput.closeEntry();
        }
      } 
      Location writeLockLocation = frameworkDir.append("write_in_progress");
      frameworkDir.mkdirs("755");

      while (!frameworkLocation.exists()) {
        if (!uploadToLocation(frameworkLocation, archive, writeLockLocation)) {
          waitForLocation(frameworkLocation, writeLockLocation);
        }
      }
    } finally {
      Files.deleteIfExists(archive.toPath());
    }

    return new SparkFramework(new LocalizeResource(resolveURI(frameworkLocation), true), SPARK_YARN_ARCHIVE);
  }

  private static boolean uploadToLocation(Location frameworkLocation, File archive, Location writeLockLocation)
    throws IOException {
    try {
      if (!writeLockLocation.createNew()) {
        return false;
      }
    } catch (IOException e) {
      LOG.debug("Failed to upload spark framework artifact while creating lock file and will be retried.", e);
      return false;
    }

    Location tempArchiveHdfs = frameworkLocation.getTempFile("temp_jar");
    try {
      //Copy file to temp loc ( Upload )
      try (OutputStream os = tempArchiveHdfs.getOutputStream()) {
        Files.copy(archive.toPath(), os);
      }
      //Move to actual location
      tempArchiveHdfs.renameTo(frameworkLocation);
      writeLockLocation.delete();
    } finally {
      tempArchiveHdfs.delete();
    }
    return true;
  }

  private static void waitForLocation(Location frameworkLocation, Location writeLockLocation)
    throws IOException {
    //In case of race condition 2nd process will wait for the 1st process to write
    //Wait for the frameworkLocation to be deleted
    try {
      Tasks.waitFor(true, frameworkLocation::exists, 5, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      //The spark jar copying was not completed by other process within the given timeout
      //Clean the artifacts from other task and retry writing
      writeLockLocation.delete();
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
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

  /**
   * Loads the enviroment variables setup by the spark-env.sh file.
   * @return
   */
  private static Map<String, String> loadSparkEnv(@Nullable String sparkHome) {
    if (sparkHome == null) {
      return Collections.emptyMap();
    }
    Path sparkEnvSh = Paths.get(sparkHome, "conf", SPARK_ENV_SH);
    if (!Files.isReadable(sparkEnvSh)) {
      LOG.debug("Skip reading {} for Spark environment due to unreadable file");
      return Collections.emptyMap();
    }

    try {
      // Run the spark-env.sh script and capture the environment variables in the output
      ProcessBuilder processBuilder = new ProcessBuilder()
        .command("/bin/sh", "-ac", String.format(". %s; printenv", sparkEnvSh.toAbsolutePath().toString()))
        .redirectErrorStream(true);

      processBuilder.environment().clear();
      Process process = processBuilder.start();

      CompletableFuture<Map<String, String>> output = new CompletableFuture<>();
      Thread t = new Thread(() -> {
        Map<String, String> envs = new HashMap<>();
        Pattern pattern = Pattern.compile("(SPARK_.*?)=(.+)");

        // Parse the output to extract environment variables
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                                                                              StandardCharsets.UTF_8))) {
          String line = reader.readLine();
          while (line != null) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
              envs.put(matcher.group(1), matcher.group(2));
            }
            line = reader.readLine();
          }

          output.complete(envs);
        } catch (IOException e) {
          output.completeExceptionally(e);
        }
      });
      t.setDaemon(true);
      t.start();

      // Should not take long for the shell command to complete
      if (!process.waitFor(10, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        LOG.warn("Failed to read Spark environment from {} with process taking longer than 10 seconds to complete",
                 sparkEnvSh);
        return Collections.emptyMap();
      }
      int exitValue = process.exitValue();
      if (exitValue != 0) {
        LOG.warn("Failed to read Spark environment from {} with process failed with exit code {}",
                 sparkEnvSh, exitValue);
        return Collections.emptyMap();
      }
      return output.get();
    } catch (Exception e) {
      LOG.warn("Failed to read Spark enviroment from {} due to failure", sparkEnvSh, e);
      return Collections.emptyMap();
    }
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
