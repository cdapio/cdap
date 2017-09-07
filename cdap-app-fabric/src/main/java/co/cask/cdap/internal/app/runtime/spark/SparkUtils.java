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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

/**
 * A utility class to help determine Spark supports and locating Spark jar.
 * TODO: CDAP-5506. Ideally this class shouldn't be in app-fabric, but should be in spark-core.
 *
 */
public final class SparkUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkUtils.class);

  // The prefix for spark environment variable names. It is being setup by the startup script.
  private static final String SPARK_ENV_PREFIX = "_SPARK_";
  // Environment variable name for the spark conf directory.
  private static final String SPARK_CONF_DIR = "SPARK_CONF_DIR";
  // Environment variable name for locating spark assembly jar file
  private static final String SPARK_ASSEMBLY_JAR = "SPARK_ASSEMBLY_JAR";
  // File name for the spark default config
  private static final String SPARK_DEFAULTS_CONF = "spark-defaults.conf";

  // Environment variable name for locating spark home directory
  public static final String SPARK_HOME = Constants.SPARK_HOME;

  // File name of the Spark conf directory as defined by the Spark framework
  // This is for the Hack to workaround CDAP-5019 (SPARK-13441)
  public static final String LOCALIZED_CONF_DIR = "__spark_conf__";

  // The Hive conf directories as determined by the startup script
  private static final String EXPLORE_CONF_DIRS = "explore.conf.dirs";

  private static Map<String, String> sparkEnv;

  private static File sparkAssemblyJar;

  /**
   * Locates the spark-assembly jar from the local file system.
   *
   * @return the spark-assembly jar location
   * @throws IllegalStateException if cannot locate the spark assembly jar
   */
  public static synchronized File locateSparkAssemblyJar() {
    if (sparkAssemblyJar != null) {
      return sparkAssemblyJar;
    }

    // If someone explicitly set the location, use it.
    // It's useful for overriding what being set for SPARK_HOME
    String jarEnv = System.getenv(SPARK_ASSEMBLY_JAR);
    if (jarEnv != null) {
      File file = new File(jarEnv);
      if (file.isFile()) {
        LOG.info("Located Spark Assembly JAR in {}", file);
        sparkAssemblyJar = file;
        return file;
      }
      LOG.warn("Env $" + SPARK_ASSEMBLY_JAR + "=" + jarEnv + " is not a file. " +
                 "Will locate Spark Assembly JAR with $" + SPARK_HOME);
    }

    String sparkHome = System.getenv(SPARK_HOME);
    if (sparkHome == null) {
      throw new IllegalStateException("Spark library not found. " +
                                        "Please set environment variable " + SPARK_HOME + " or " + SPARK_ASSEMBLY_JAR);
    }

    // Look for spark-assembly.jar symlink
    Path assemblyJar = Paths.get(sparkHome, "lib", "spark-assembly.jar");
    if (Files.isSymbolicLink(assemblyJar)) {
      sparkAssemblyJar = assemblyJar.toFile();
      return sparkAssemblyJar;
    }

    // No symbolic link exists. Search for spark-assembly*.jar in the lib directory
    Path sparkLib = Paths.get(sparkHome, "lib");
    final PathMatcher pathMatcher = sparkLib.getFileSystem().getPathMatcher("glob:spark-assembly*.jar");
    try {
      Files.walkFileTree(sparkLib, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          // Take the first file match
          if (attrs.isRegularFile() && pathMatcher.matches(file.getFileName())) {
            sparkAssemblyJar = file.toFile();
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

    Preconditions.checkState(sparkAssemblyJar != null, "Failed to locate Spark library from %s", sparkHome);

    LOG.info("Located Spark Assembly JAR in {}", sparkAssemblyJar);
    return sparkAssemblyJar;
  }

  /**
   * Prepares the resources that need to be localized to the Spark client container.
   *
   * @param tempDir a temporary directory for file creation
   * @param localizeResources A map from localized name to {@link LocalizeResource} for this method to update
   * @return localized name of the Spark assembly jar file
   */
  public static String prepareSparkResources(File tempDir, Map<String, LocalizeResource> localizeResources) {
    File sparkAssemblyJar = locateSparkAssemblyJar();
    localizeResources.put(sparkAssemblyJar.getName(), new LocalizeResource(sparkAssemblyJar));

    // Localize the spark-defaults.conf file if it exists.
    File sparkDefaultConfFile = locateSparkDefaultsConfFile(getSparkEnv());
    if (sparkDefaultConfFile != null) {
      localizeResources.put(sparkDefaultConfFile.getName(), new LocalizeResource(sparkDefaultConfFile));
    }

    // Shallow copy all files under directory defined by $HADOOP_CONF_DIR and the explore conf directory
    // If $HADOOP_CONF_DIR is not defined, use the location of "yarn-site.xml" to determine the directory
    // This is part of workaround for CDAP-5019 (SPARK-13441) and CDAP-12330
    List<File> configDirs = new ArrayList<>();

    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())) {
      configDirs.add(new File(System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())));
    } else {
      URL yarnSiteLocation = SparkUtils.class.getClassLoader().getResource("yarn-site.xml");
      if (yarnSiteLocation == null || !"file".equals(yarnSiteLocation.getProtocol())) {
        LOG.warn("Failed to derive HADOOP_CONF_DIR from yarn-site.xml location: {}", yarnSiteLocation);
      } else {
        configDirs.add(new File(yarnSiteLocation.getPath()).getParentFile());
      }
    }

    // Include the explore config dirs as well
    Splitter splitter = Splitter.on(File.pathSeparatorChar).omitEmptyStrings();
    for (String dir: splitter.split(System.getProperty(EXPLORE_CONF_DIRS, ""))) {
      configDirs.add(new File(dir));
    }

    if (!configDirs.isEmpty()) {
      try {
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
      } catch (IOException e) {
        LOG.warn("Failed to create {}.zip file. Spark program execution may fail, " +
                   "depending on the hadoop distribution version", LOCALIZED_CONF_DIR);
      }
    }

    return sparkAssemblyJar.getName();
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

      // Rewrite the hive-site.xml file to set "hive.metastore.token.signature" to "hiveserver2ClientToken"
      if ("hive-site.xml".equals(file.getName())) {
        Configuration conf = new Configuration(false);
        conf.clear();
        conf.addResource(file.toURI().toURL());
        conf.set("hive.metastore.token.signature", "hiveserver2ClientToken");
        conf.writeXml(zipOutput);
      } else {
        Files.copy(file.toPath(), zipOutput);
      }

      zipOutput.closeEntry();
    }
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
    env.put("YARN_CONF_DIR", "$PWD/" + LOCALIZED_CONF_DIR);
    env.remove(SPARK_HOME);

    // Spark using YARN and it is needed for both Workflow and Spark runner. We need to set it
    // because inside Spark code, it will set and unset the SPARK_YARN_MODE system properties, causing
    // fork in distributed mode not working. Setting it in the environment, which Spark uses for defaults,
    // so it can't be unset by Spark
    env.put("SPARK_YARN_MODE", "true");

    return Collections.unmodifiableMap(env);
  }

  @Nullable
  public static File locateSparkDefaultsConfFile(Map<String, String> env) {
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

  private SparkUtils() {
  }
}
