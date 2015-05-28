/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.format.RecordFormats;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.hive.Hive12ExploreService;
import co.cask.cdap.explore.service.hive.Hive13ExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import co.cask.cdap.explore.service.hive.HiveCDH4ExploreService;
import co.cask.cdap.explore.service.hive.HiveCDH5ExploreService;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceUtils.class);

  /**
   * Hive support enum.
   */
  public enum HiveSupport {
    HIVE_CDH4(Pattern.compile("^.*cdh4\\..*$"), HiveCDH4ExploreService.class),
    HIVE_CDH5(Pattern.compile("^.*cdh5\\..*$"), HiveCDH5ExploreService.class),

    HIVE_12(null, Hive12ExploreService.class),
    HIVE_13(null, Hive13ExploreService.class),
    HIVE_14(null, Hive14ExploreService.class),
    HIVE_1_0(null, Hive14ExploreService.class);

    private final Pattern hadoopVersionPattern;
    private final Class<? extends ExploreService> hiveExploreServiceClass;

    HiveSupport(Pattern hadoopVersionPattern, Class<? extends ExploreService> hiveExploreServiceClass) {
      this.hadoopVersionPattern = hadoopVersionPattern;
      this.hiveExploreServiceClass = hiveExploreServiceClass;
    }

    public Pattern getHadoopVersionPattern() {
      return hadoopVersionPattern;
    }

    public Class<? extends ExploreService> getHiveExploreServiceClass() {
      return hiveExploreServiceClass;
    }
  }

  // Caching the dependencies so that we don't trace them twice
  private static Set<File> exploreDependencies = null;
  // Caching explore class loader
  private static ClassLoader exploreClassLoader = null;

  private static final Pattern HIVE_SITE_FILE_PATTERN = Pattern.compile("^.*/hive-site\\.xml$");

  /**
   * Get all the files contained in a class path.
   */
  public static Iterable<File> getClassPathJarsFiles(String hiveClassPath) {
    if (hiveClassPath == null) {
      return null;
    }
    return Iterables.transform(Splitter.on(':').split(hiveClassPath), STRING_FILE_FUNCTION);
  }

  private static final Function<String, File> STRING_FILE_FUNCTION =
    new Function<String, File>() {
      @Override
      public File apply(String input) {
        return new File(input).getAbsoluteFile();
      }
    };

  /**
   * Builds a class loader with the class path provided.
   */
  public static ClassLoader getExploreClassLoader() {
    if (exploreClassLoader != null) {
      return exploreClassLoader;
    }

    // EXPLORE_CLASSPATH and EXPLORE_CONF_FILES will be defined in startup scripts if Hive is installed.
    String exploreClassPathStr = System.getProperty(Constants.Explore.EXPLORE_CLASSPATH);
    LOG.debug("Explore classpath = {}", exploreClassPathStr);
    if (exploreClassPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CLASSPATH + " is not set.");
    }

    String exploreConfPathStr = System.getProperty(Constants.Explore.EXPLORE_CONF_FILES);
    LOG.debug("Explore confPath = {}", exploreConfPathStr);
    if (exploreConfPathStr == null) {
      throw new RuntimeException("System property " + Constants.Explore.EXPLORE_CONF_FILES + " is not set.");
    }

    Iterable<File> hiveClassPath = getClassPathJarsFiles(exploreClassPathStr);
    Iterable<File> hiveConfFiles = getClassPathJarsFiles(exploreConfPathStr);
    ImmutableList.Builder<URL> builder = ImmutableList.builder();
    for (File file : Iterables.concat(hiveClassPath, hiveConfFiles)) {
      try {
        if (file.getName().matches(".*\\.xml")) {
          builder.add(file.getParentFile().toURI().toURL());
        } else {
          builder.add(file.toURI().toURL());
        }
      } catch (MalformedURLException e) {
        LOG.error("Jar URL is malformed", e);
        throw Throwables.propagate(e);
      }
    }
    exploreClassLoader = new URLClassLoader(Iterables.toArray(builder.build(), URL.class),
                                            ClassLoader.getSystemClassLoader());
    return exploreClassLoader;
  }

  public static Class<? extends ExploreService> getHiveService() {
    HiveSupport hiveVersion = checkHiveSupport(null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  public static HiveSupport checkHiveSupport() {
    return checkHiveSupport(getExploreClassLoader());
  }

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static HiveSupport checkHiveSupport(ClassLoader hiveClassLoader) {
    // First try to figure which hive support is relevant based on Hadoop distribution name
    String hadoopVersion = VersionInfo.getVersion();
    LOG.info("Hadoop version is: {}", hadoopVersion);
    for (HiveSupport hiveSupport : HiveSupport.values()) {
      if (hiveSupport.getHadoopVersionPattern() != null &&
        hiveSupport.getHadoopVersionPattern().matcher(hadoopVersion).matches()) {
        return hiveSupport;
      }
    }

    ClassLoader usingCL = hiveClassLoader;
    if (usingCL == null) {
      usingCL = ExploreServiceUtils.class.getClassLoader();
    }

    try {
      Class<?> hiveVersionInfoClass = usingCL.loadClass("org.apache.hive.common.util.HiveVersionInfo");
      String hiveVersion = (String) hiveVersionInfoClass.getDeclaredMethod("getVersion").invoke(null);
      if (hiveVersion.startsWith("0.12.")) {
        return HiveSupport.HIVE_12;
      } else if (hiveVersion.startsWith("0.13.")) {
        return HiveSupport.HIVE_13;
      } else if (hiveVersion.startsWith("0.14.") || hiveVersion.startsWith("1.0.")) {
        return HiveSupport.HIVE_14;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    throw new RuntimeException("Hive distribution not supported. Set the configuration '" +
                                 Constants.Explore.EXPLORE_ENABLED +
                                 "' to false to start up without Explore.");
  }

  /**
   * Return the list of absolute paths of the bootstrap classes.
   */
  public static Set<String> getBoostrapClasses() {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String classpath : Splitter.on(File.pathSeparatorChar).split(System.getProperty("sun.boot.class.path"))) {
      File file = new File(classpath);
      builder.add(file.getAbsolutePath());
      try {
        builder.add(file.getCanonicalPath());
      } catch (IOException e) {
        LOG.warn("Could not add canonical path to aux class path for file {}", file.toString(), e);
      }
    }
    return builder.build();
  }

  /**
   * Trace the jar dependencies needed by the Explore container. Uses a separate class loader to load Hive classes,
   * built using the explore classpath passed as a system property to master.
   *
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies() throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader classLoader = getExploreClassLoader();
    return traceExploreDependencies(classLoader);
  }

  /**
   * Trace the jar dependencies needed by the Explore container.
   *
   * @param classLoader class loader to use to trace the dependencies.
   *                    If it is null, use the class loader of this class.
   * @return an ordered set of jar files.
   */
  public static Set<File> traceExploreDependencies(ClassLoader classLoader)
    throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader usingCL = classLoader;
    if (classLoader == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }
    Set<String> bootstrapClassPaths = getBoostrapClasses();

    Set<File> hBaseTableDeps = traceDependencies(HBaseTableUtilFactory.getHBaseTableUtilClass().getName(),
                                                 bootstrapClassPaths, usingCL);

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(traceDependencies(DatasetService.class.getName(),
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("co.cask.cdap.hive.datasets.DatasetStorageHandler",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("co.cask.cdap.hive.datasets.StreamStorageHandler",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hadoop.hive.ql.exec.mr.ExecDriver",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hive.service.cli.CLIService",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies("org.apache.hadoop.mapred.YarnClientProtocolProvider",
                                                 bootstrapClassPaths, usingCL));
    orderedDependencies.addAll(traceDependencies(RecordFormats.class.getName(),
                                                 bootstrapClassPaths, usingCL));

    // Needed for - at least - CDH 4.4 integration
    orderedDependencies.addAll(traceDependencies("org.apache.hive.builtins.BuiltinUtils",
                                                 bootstrapClassPaths, usingCL));

    // Needed for - at least - CDH 5 integration
    orderedDependencies.addAll(traceDependencies("org.apache.hadoop.hive.shims.Hadoop23Shims",
                                                 bootstrapClassPaths, usingCL));

    exploreDependencies = orderedDependencies;
    return orderedDependencies;
  }

  /**
   * Trace the dependencies files of the given className, using the classLoader,
   * and excluding any class contained in the bootstrapClassPaths and Kryo classes.
   * We need to remove Kryo dependency in the Explore container. Spark introduced version 2.21 version of Kryo,
   * which would be normally shipped to the Explore container. Yet, Hive requires Kryo 2.22,
   * and gets it from the Hive jars - hive-exec.jar to be precise.
   *
   * Nothing is returned if the classLoader does not contain the className.
   */
  public static Set<File> traceDependencies(String className, final Set<String> bootstrapClassPaths,
                                            ClassLoader classLoader)
    throws IOException {
    ClassLoader usingCL = classLoader;
    if (usingCL == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }
    final Set<File> jarFiles = Sets.newHashSet();

    Dependencies.findClassDependencies(
      usingCL,
      new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (bootstrapClassPaths.contains(classPathUrl.getFile())) {
            return false;
          }

          if (className.startsWith("com.esotericsoftware.kryo")) {
            return false;
          }

          jarFiles.add(new File(classPathUrl.getFile()));
          return true;
        }
      },
      className
    );

    return jarFiles;
  }

  /**
   * Check that the file is a hive-site.xml file, and return a temp copy of it to which are added
   * necessary options. If it is not a hive-site.xml file, return it as is.
   */
  public static File hijackHiveConfFile(File confFile) {
    if (!HIVE_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return confFile;
    }

    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    // Prefer our job jar in the classpath
    // Set both old and new keys
    // Those settings will be in hive-site.xml in the classpath of the Explore Service. Therefore,
    // all HiveConf objects created there will have those settings, and they will be passed to
    // the map reduces jobs launched by Hive.
    conf.setBoolean("mapreduce.user.classpath.first", true);
    conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    File newHiveConfFile = new File(Files.createTempDir(), "hive-site.xml");
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(newHiveConfFile);
    } catch (FileNotFoundException e) {
      LOG.error("Problem creating temporary hive-site.xml conf file at {}", newHiveConfFile, e);
      throw Throwables.propagate(e);
    }

    try {
      conf.writeXml(fos);
    } catch (IOException e) {
      LOG.error("Could not write modified configuration to temporary hive-site.xml at {}", newHiveConfFile, e);
      throw Throwables.propagate(e);
    } finally {
      Closeables.closeQuietly(fos);
    }

    return newHiveConfFile;
  }
}
