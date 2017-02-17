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
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreRuntimeModule;
import co.cask.cdap.explore.service.hive.Hive12CDH5ExploreService;
import co.cask.cdap.explore.service.hive.Hive12ExploreService;
import co.cask.cdap.explore.service.hive.Hive13ExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import co.cask.cdap.hive.ExploreUtils;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.internal.utils.Dependencies;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceUtils.class);

  private static final String HIVE_AUTHFACTORY_CLASS_NAME = "org.apache.hive.service.auth.HiveAuthFactory";

  /**
   * Hive support enum.
   */
  public enum HiveSupport {
    // The order of the enum values below is very important
    // CDH 5.0 to 5.1 uses Hive 0.12
    HIVE_CDH5_0(Pattern.compile("^.*cdh5.0\\..*$"), Hive12CDH5ExploreService.class),
    HIVE_CDH5_1(Pattern.compile("^.*cdh5.1\\..*$"), Hive12CDH5ExploreService.class),
    // CDH 5.2.x and 5.3.x use Hive 0.13
    HIVE_CDH5_2(Pattern.compile("^.*cdh5.2\\..*$"), Hive13ExploreService.class),
    HIVE_CDH5_3(Pattern.compile("^.*cdh5.3\\..*$"), Hive13ExploreService.class),
    // CDH > 5.3 uses Hive >= 1.1 (which Hive14ExploreService supports)
    HIVE_CDH5(Pattern.compile("^.*cdh5\\..*$"), Hive14ExploreService.class),

    HIVE_12(null, Hive12ExploreService.class),
    HIVE_13(null, Hive13ExploreService.class),
    HIVE_14(null, Hive14ExploreService.class),
    HIVE_1_0(null, Hive14ExploreService.class),
    HIVE_1_1(null, Hive14ExploreService.class),
    HIVE_1_2(null, Hive14ExploreService.class);

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

  private static final Pattern HIVE_SITE_FILE_PATTERN = Pattern.compile("^.*/hive-site\\.xml$");
  private static final Pattern YARN_SITE_FILE_PATTERN = Pattern.compile("^.*/yarn-site\\.xml$");
  private static final Pattern MAPRED_SITE_FILE_PATTERN = Pattern.compile("^.*/mapred-site\\.xml$");
  private static final Pattern TEZ_SITE_FILE_PATTERN = Pattern.compile("^.*/tez-site\\.xml$");

  public static Class<? extends ExploreService> getHiveService() {
    HiveSupport hiveVersion = checkHiveSupport(null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  public static boolean shouldEscapeColumns(Configuration hConf) {
    // backtick support was added in Hive13.
    ExploreServiceUtils.HiveSupport hiveSupport = ExploreServiceUtils.checkHiveSupport(hConf.getClassLoader());
    if (hiveSupport == HiveSupport.HIVE_12
      || hiveSupport == HiveSupport.HIVE_CDH5_0
      || hiveSupport == HiveSupport.HIVE_CDH5_1) {
      return false;
    }

    // if this is set to false, we don't need to escape the columns.
    if (!hConf.getBoolean("hive.support.sql11.reserved.keywords", true)) {
      return false;
    }

    // otherwise, if this setting is set to 'column', escape all column names
    return "column".equalsIgnoreCase(hConf.get("hive.support.quoted.identifiers", "column"));
  }

  public static HiveSupport checkHiveSupport() {
    return checkHiveSupport(ExploreUtils.getExploreClassloader());
  }

  public static String getHiveVersion() {
    return getHiveVersion(ExploreUtils.getExploreClassloader());
  }

  public static String getHiveVersion(@Nullable ClassLoader hiveClassLoader) {
    ClassLoader usingCL = hiveClassLoader;
    if (usingCL == null) {
      usingCL = ExploreServiceUtils.class.getClassLoader();
    }

    try {
      Class<?> hiveVersionInfoClass = usingCL.loadClass("org.apache.hive.common.util.HiveVersionInfo");
      return (String) hiveVersionInfoClass.getDeclaredMethod("getVersion").invoke(null);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Check that Hive is in the class path - with a right version.
   */
  public static HiveSupport checkHiveSupport(@Nullable ClassLoader hiveClassLoader) {
    // First try to figure which hive support is relevant based on Hadoop distribution name
    String hadoopVersion = VersionInfo.getVersion();
    for (HiveSupport hiveSupport : HiveSupport.values()) {
      if (hiveSupport.getHadoopVersionPattern() != null &&
        hiveSupport.getHadoopVersionPattern().matcher(hadoopVersion).matches()) {
        return hiveSupport;
      }
    }

    String hiveVersion = getHiveVersion(hiveClassLoader);
    LOG.debug("Client Hive version: {}", hiveVersion);
    if (hiveVersion.startsWith("0.12.")) {
      return HiveSupport.HIVE_12;
    } else if (hiveVersion.startsWith("0.13.")) {
      return HiveSupport.HIVE_13;
    } else if (hiveVersion.startsWith("0.14.") || hiveVersion.startsWith("1.0.")) {
      return HiveSupport.HIVE_14;
    } else if (hiveVersion.startsWith("1.1.")) {
      return HiveSupport.HIVE_1_1;
    }  else if (hiveVersion.startsWith(("1.2"))) {
      return HiveSupport.HIVE_1_2;
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
  public static Set<File> traceExploreDependencies(File tmpDir) throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader classLoader = ExploreUtils.getExploreClassloader();
    Set<File> additionalJars = new HashSet<>();
    if (isSparkAvailable()) {
      File sparkAssemblyJar = SparkUtils.locateSparkAssemblyJar();
      LOG.debug("Adding spark jar to explore dependency {}", sparkAssemblyJar);
      additionalJars.add(sparkAssemblyJar);
    }
    if (isTezAvailable()) {
      additionalJars.addAll(getTezJars());
    }
    return traceExploreDependencies(classLoader, tmpDir, additionalJars);
  }

  /**
   * Trace the jar dependencies needed by the Explore container.
   *
   * @param classLoader class loader to use to trace the dependencies.
   *                    If it is null, use the class loader of this class.
   * @param tmpDir temporary directory for storing rewritten jar files.
   * @param additionalJars additional jars that will be added to the end of the returned set.
   * @return an ordered set of jar files.
   */
  private static Set<File> traceExploreDependencies(ClassLoader classLoader, File tmpDir, Set<File> additionalJars)
    throws IOException {
    if (exploreDependencies != null) {
      return exploreDependencies;
    }

    ClassLoader usingCL = classLoader;
    if (classLoader == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }

    final Set<String> bootstrapClassPaths = getBoostrapClasses();

    ClassAcceptor classAcceptor = new ClassAcceptor() {
      /* Excluding any class contained in the bootstrapClassPaths and Kryo classes.
        * We need to remove Kryo dependency in the Explore container. Spark introduced version 2.21 version of Kryo,
        * which would be normally shipped to the Explore container. Yet, Hive requires Kryo 2.22,
        * and gets it from the Hive jars - hive-exec.jar to be precise.
        * */
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        return !(bootstrapClassPaths.contains(classPathUrl.getFile()) ||
          className.startsWith("com.esotericsoftware.kryo"));
      }
    };

    Set<File> hBaseTableDeps = traceDependencies(usingCL, classAcceptor, tmpDir,
                                                 HBaseTableUtilFactory.getHBaseTableUtilClass().getName());

    // Note the order of dependency jars is important so that HBase jars come first in the classpath order
    // LinkedHashSet maintains insertion order while removing duplicate entries.
    Set<File> orderedDependencies = new LinkedHashSet<>();
    orderedDependencies.addAll(hBaseTableDeps);
    orderedDependencies.addAll(traceDependencies(usingCL, classAcceptor, tmpDir,
                                                 DatasetService.class.getName(),
                                                 // Referred to by string rather than Class.getName()
                                                 // because DatasetStorageHandler and StreamStorageHandler
                                                 // extend a Hive class, which isn't present in this class loader
                                                 "co.cask.cdap.hive.datasets.DatasetStorageHandler",
                                                 "co.cask.cdap.hive.stream.StreamStorageHandler",
                                                 "org.apache.hadoop.hive.ql.exec.mr.ExecDriver",
                                                 "org.apache.hive.service.cli.CLIService",
                                                 "org.apache.hadoop.mapred.YarnClientProtocolProvider",
                                                 // Needed for - at least - CDH 4.4 integration
                                                 "org.apache.hive.builtins.BuiltinUtils",
                                                 // Needed for - at least - CDH 5 integration
                                                 "org.apache.hadoop.hive.shims.Hadoop23Shims"));
    orderedDependencies.addAll(additionalJars);

    exploreDependencies = orderedDependencies;
    return orderedDependencies;
  }

  /**
   * Trace the dependencies files of the given className, using the classLoader,
   * and including the classes that's accepted by the classAcceptor
   *
   * Nothing is returned if the classLoader, or if not provided, the ExploreRuntimeModule class loader,
   * does not contain the className.
   */
  public static Set<File> traceDependencies(@Nullable ClassLoader classLoader, final ClassAcceptor classAcceptor,
                                            File tmpDir, String... classNames) throws IOException {
    LOG.debug("Tracing dependencies for classes: {}", Arrays.toString(classNames));

    ClassLoader usingCL = classLoader;
    if (usingCL == null) {
      usingCL = ExploreRuntimeModule.class.getClassLoader();
    }

    final String rewritingClassName = HIVE_AUTHFACTORY_CLASS_NAME;
    final Set<File> rewritingFiles = Sets.newHashSet();

    final Set<File> jarFiles = Sets.newHashSet();
    Dependencies.findClassDependencies(
      usingCL,
      new ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          if (!classAcceptor.accept(className, classUrl, classPathUrl)) {
            return false;
          }

          if (rewritingClassName.equals(className)) {
            rewritingFiles.add(new File(classPathUrl.getFile()));
          }

          jarFiles.add(new File(classPathUrl.getFile()));
          return true;
        }
      },
      classNames
    );

    // Rewrite HiveAuthFactory.loginFromKeytab to be a no-op method.
    // This is needed because we don't want to use Hive's CLIService since
    // we're already using delegation tokens
    for (File rewritingFile : rewritingFiles) {
      // TODO: this may cause lots of rewrites since we may rewrite the same jar multiple times
      File rewrittenJar = rewriteHiveAuthFactory(
        rewritingFile, new File(tmpDir, rewritingFile.getName() + "-" + System.currentTimeMillis() + ".jar"));
      jarFiles.add(rewrittenJar);
      LOG.debug("Rewrote {} to {}", rewritingFile.getAbsolutePath(), rewrittenJar.getAbsolutePath());
    }
    jarFiles.removeAll(rewritingFiles);

    if (LOG.isDebugEnabled()) {
      for (File jarFile : jarFiles) {
        LOG.debug("Added jar {}", jarFile.getAbsolutePath());
      }
    }

    return jarFiles;
  }

  @VisibleForTesting
  static File rewriteHiveAuthFactory(File sourceJar, File targetJar) throws IOException {
    try (
      JarFile input = new JarFile(sourceJar);
      JarOutputStream output = new JarOutputStream(new FileOutputStream(targetJar))
    ) {
      String hiveAuthFactoryPath = HIVE_AUTHFACTORY_CLASS_NAME.replace('.', '/') + ".class";

      Enumeration<JarEntry> sourceEntries = input.entries();
      while (sourceEntries.hasMoreElements()) {
        JarEntry entry = sourceEntries.nextElement();
        output.putNextEntry(new JarEntry(entry.getName()));

        try (InputStream entryInputStream = input.getInputStream(entry)) {
          if (!hiveAuthFactoryPath.equals(entry.getName())) {
            ByteStreams.copy(entryInputStream, output);
            continue;
          }

          try {
            // Rewrite the bytecode of HiveAuthFactory.loginFromKeytab method to a no-op method
            ClassReader cr = new ClassReader(entryInputStream);
            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
              @Override
              public MethodVisitor visitMethod(final int access, final String name, final String desc,
                                               String signature, String[] exceptions) {
                MethodVisitor methodVisitor = super.visitMethod(access, name, desc, signature, exceptions);
                if (!"loginFromKeytab".equals(name)) {
                  return methodVisitor;
                }
                GeneratorAdapter adapter = new GeneratorAdapter(methodVisitor, access, name, desc);
                adapter.returnValue();

                // VisitMaxs with 0 so that COMPUTE_MAXS from ClassWriter will compute the right values.
                adapter.visitMaxs(0, 0);
                return new MethodVisitor(Opcodes.ASM5) { };
              }
            }, 0);
            output.write(cw.toByteArray());
          } catch (Exception e) {
            throw new IOException("Unable to generate HiveAuthFactory class", e);
          }
        }
      }

      return targetJar;
    }
  }

  /**
   * Updates environment variables in hive-site.xml, mapred-site.xml and yarn-site.xml for explore.
   * All other conf files are returned without any update.
   * @param confFile conf file to update
   * @param tempDir temp dir to create files if necessary
   * @param cdapJars set of cdap jar files to be added in the beginning of the classpath
   * @return the new conf file to use in place of confFile
   */
  public static File updateConfFileForExplore(File confFile, File tempDir, Set<File> cdapJars) {
    if (HIVE_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateHiveConfFile(confFile, tempDir);
    } else if (YARN_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateYarnConfFile(confFile, tempDir, cdapJars);
    } else if (MAPRED_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateMapredConfFile(confFile, tempDir, cdapJars);
    } else if (TEZ_SITE_FILE_PATTERN.matcher(confFile.getAbsolutePath()).matches()) {
      return updateTezConfFile(confFile, tempDir, cdapJars);
    } else {
      return confFile;
    }
  }

  /**
   * Change yarn-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateYarnConfFile(File confFile, File tempDir, Set<File> cdapJars) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    String yarnAppClassPath = conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                       Joiner.on(",").join(YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH));

    // We add cdap jars to the classpath in the beginning. We then add the pwd/* to the classpath.
    // So user's jar will take precedence. Without pwd/* in the beginning of classpath, job.jar will be at
    // the beginning of the classpath. Since job.jar has old guava version classes, we want to add pwd/* before.
    String cdapJarsClassPath = "";
    for (File cdapJar : cdapJars) {
      cdapJarsClassPath = cdapJarsClassPath + "$PWD/" + cdapJar.getName() + ",";
    }

    yarnAppClassPath = cdapJarsClassPath + "$PWD/*," + yarnAppClassPath;

    LOG.debug("Setting yarn.application.classpath to {}", yarnAppClassPath);
    conf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, yarnAppClassPath);

    File newYarnConfFile = new File(tempDir, "yarn-site.xml");
    try (FileOutputStream os = new FileOutputStream(newYarnConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating and writing to temporary yarn-conf.xml conf file at {}", newYarnConfFile, e);
      throw Throwables.propagate(e);
    }

    return newYarnConfFile;
  }

  /**
   * Change mapred-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateMapredConfFile(File confFile, File tempDir, Set<File> cdapJars) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    String mrAppClassPath = conf.get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                                     MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);

    // We add cdap jars to the classpath in the beginning. We then add the pwd/* to the classpath.
    // so user's jar will take precedence. Without pwd/* in the beginning of classpath, job.jar will be at
    // the beginning of the classpath. Since job.jar has old guava version classes, we want to add pwd/* before.
    String cdapJarsClassPath = "";
    for (File cdapJar : cdapJars) {
      cdapJarsClassPath = cdapJarsClassPath + "$PWD/" + cdapJar.getName() + ",";
    }
    mrAppClassPath = cdapJarsClassPath + "$PWD/*," + mrAppClassPath;

    LOG.debug("Setting mapreduce.application.classpath to {}", mrAppClassPath);
    conf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, mrAppClassPath);

    File newMapredConfFile = new File(tempDir, "mapred-site.xml");
    try (FileOutputStream os = new FileOutputStream(newMapredConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating and writing to temporary mapred-site.xml conf file at {}", newMapredConfFile, e);
      throw Throwables.propagate(e);
    }

    return newMapredConfFile;
  }

  /**
   * Change tez-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateTezConfFile(File confFile, File tempDir, Set<File> cdapJars) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    // We add cdap jars to the classpath in the beginning.
    String tezClassPath = conf.get(TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX);

    String cdapJarsClassPath = "";
    for (File cdapJar : cdapJars) {
      // Note that Tez expects the classpath seperator as ":"
      cdapJarsClassPath = cdapJarsClassPath + "$PWD/" + cdapJar.getName() + File.pathSeparator;
    }
    String additionalClassPath = cdapJarsClassPath;
    if (tezClassPath != null && !tezClassPath.trim().isEmpty()) {
      additionalClassPath = cdapJarsClassPath + tezClassPath;
    }

    LOG.debug(String.format("Setting %s to %s",
                            TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX, additionalClassPath));

    conf.set(TezConfiguration.TEZ_CLUSTER_ADDITIONAL_CLASSPATH_PREFIX, additionalClassPath);
    File newTezConfFile = new File(tempDir, "tez-site.xml");
    try (FileOutputStream os = new FileOutputStream(newTezConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating and writing to temporary tez-site.xml conf file at {}", newTezConfFile, e);
      throw Throwables.propagate(e);
    }

    return newTezConfFile;
  }

  /**
   * Change hive-site.xml file, and return a temp copy of it to which are added
   * necessary options.
   */
  private static File updateHiveConfFile(File confFile, File tempDir) {
    Configuration conf = new Configuration(false);
    try {
      conf.addResource(confFile.toURI().toURL());
    } catch (MalformedURLException e) {
      LOG.error("File {} is malformed.", confFile, e);
      throw Throwables.propagate(e);
    }

    // we prefer jars at container's root directory before job.jar,
    // we edit the YARN_APPLICATION_CLASSPATH in yarn-site.xml using
    // co.cask.cdap.explore.service.ExploreServiceUtils.updateYarnConfFile and
    // setting the MAPREDUCE_JOB_CLASSLOADER and MAPREDUCE_JOB_USER_CLASSPATH_FIRST to false will put
    // YARN_APPLICATION_CLASSPATH before job.jar for container's classpath.
    conf.setBoolean(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, false);
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, false);

    String sparkHome = System.getenv(Constants.SPARK_HOME);
    if (sparkHome != null) {
      LOG.debug("Setting spark.home in hive conf to {}", sparkHome);
      conf.set("spark.home", sparkHome);
    }

    File newHiveConfFile = new File(tempDir, "hive-site.xml");

    try (FileOutputStream os = new FileOutputStream(newHiveConfFile)) {
      conf.writeXml(os);
    } catch (IOException e) {
      LOG.error("Problem creating temporary hive-site.xml conf file at {}", newHiveConfFile, e);
      throw Throwables.propagate(e);
    }
    return newHiveConfFile;
  }

  private static boolean isSparkAvailable() {
    try {
      // SparkUtils.locateSparkAssemblyJar() throws IllegalStateException if it is not able to locate spark jar
      SparkUtils.locateSparkAssemblyJar();
      return true;
    } catch (IllegalStateException e) {
      LOG.debug("Got exception while determining spark availability", e);
      return false;
    }
  }

  public static boolean isSparkEngine(HiveConf hiveConf) {
    // We don't support setting engine through session configuration now
    String engine = hiveConf.get("hive.execution.engine");
    return "spark".equals(engine);
  }

  public static boolean isTezEngine(HiveConf hiveConf) {
    // We don't support setting engine through session configuration now
    String engine = hiveConf.get("hive.execution.engine");
    return "tez".equals(engine);
  }

  // This method is used to determine if Tez is enabled based on TEZ_HOME environment variable.
  // Master prepares the explore container by adding jars available in TEZ_HOME.
  // However this environment variable is not available to explore container itself(BaseHiveService).
  // There we check the existence of tez using hive.execution.engine config variable.
  private static boolean isTezAvailable() {
    return System.getenv(Constants.TEZ_HOME) != null;
  }

  private static Set<File> getTezJars() {
    String tezHome = System.getenv(Constants.TEZ_HOME);
    Path tezHomeDir = Paths.get(tezHome);
    final PathMatcher pathMatcher = tezHomeDir.getFileSystem().getPathMatcher("glob:*.jar");
    final Set<File> tezJars = new HashSet<>();
    try {
      Files.walkFileTree(tezHomeDir, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          if (attrs.isRegularFile() && pathMatcher.matches(file.getFileName())) {
            tezJars.add(file.toFile());
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
      LOG.warn("Exception raised while inspecting {}", tezHomeDir, e);
    }
    return tezJars;
  }
}
