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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.hive.Hive12CDH5ExploreService;
import co.cask.cdap.explore.service.hive.Hive12ExploreService;
import co.cask.cdap.explore.service.hive.Hive13ExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import co.cask.cdap.hive.ExploreUtils;
import co.cask.cdap.internal.asm.Classes;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
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

  private static final String HIVE_EXECUTION_ENGINE = "hive.execution.engine";

  private static final Map<String, Set<String>> HIVE_CLASS_FILES_TO_PATCH = ImmutableMap.of(
    "org/apache/hive/service/auth/HiveAuthFactory.class", Collections.singleton("loginFromKeytab"),
    "org/apache/hadoop/hive/ql/session/SessionState.class", Collections.singleton("loadAuxJars")
  );

  private static final String CDH = "cdh";

  /**
   * Hive support enum.
   */
  private enum HiveSupport {
    // The order of the enum values below is very important
    // CDH 5.0 to 5.1 uses Hive 0.12
    HIVE_CDH5_0(Pattern.compile("^.*cdh5.0\\..*$"), Hive12CDH5ExploreService.class),
    HIVE_CDH5_1(Pattern.compile("^.*cdh5.1\\..*$"), Hive12CDH5ExploreService.class),
    // CDH 5.2.x and 5.3.x use Hive 0.13
    HIVE_CDH5_2(Pattern.compile("^.*cdh5.2\\..*$"), Hive13ExploreService.class),
    HIVE_CDH5_3(Pattern.compile("^.*cdh5.3\\..*$"), Hive13ExploreService.class),
    // CDH > 5.3 uses Hive >= 1.1 (which Hive14ExploreService supports)
    HIVE_CDH5(Pattern.compile("^.*cdh5\\..*$"), Hive14ExploreService.class),
    // Current latest CDH version uses Hive >= 1.1. Need to update HIVE_CDH_LATEST when newer CDH version is added.
    HIVE_CDH_LATEST(null, Hive14ExploreService.class),

    HIVE_12(null, Hive12ExploreService.class),
    HIVE_13(null, Hive13ExploreService.class),
    HIVE_14(null, Hive14ExploreService.class),
    HIVE_1_0(null, Hive14ExploreService.class),
    HIVE_1_1(null, Hive14ExploreService.class),
    HIVE_1_2(null, Hive14ExploreService.class),
    // Current latest non-CDH version is HIVE_1_2. Need to update HIVE_LATEST when newer non-CDH version is added.
    HIVE_LATEST(HIVE_1_2);

    private final Pattern hadoopVersionPattern;
    private final Class<? extends ExploreService> hiveExploreServiceClass;

    HiveSupport(HiveSupport hiveSupport) {
      this.hadoopVersionPattern = hiveSupport.getHadoopVersionPattern();
      this.hiveExploreServiceClass = hiveSupport.getHiveExploreServiceClass();
    }

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

  public static Class<? extends ExploreService> getHiveService(CConfiguration cConf) {
    HiveSupport hiveVersion = checkHiveSupport(cConf, null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  static boolean shouldEscapeColumns(CConfiguration cConf, Configuration hConf) {
    // backtick support was added in Hive13.
    ExploreServiceUtils.HiveSupport hiveSupport = checkHiveSupport(cConf, hConf.getClassLoader());
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

  public static HiveSupport checkHiveSupport(CConfiguration cConf) {
    return checkHiveSupport(cConf, ExploreUtils.getExploreClassloader());
  }

  public static String getHiveVersion() {
    return getHiveVersion(ExploreUtils.getExploreClassloader());
  }

  private static String getHiveVersion(@Nullable ClassLoader hiveClassLoader) {
    ClassLoader usingCL = Objects.firstNonNull(hiveClassLoader, ExploreServiceUtils.class.getClassLoader());
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
  public static HiveSupport checkHiveSupport(CConfiguration cConf, @Nullable ClassLoader hiveClassLoader) {
    // First try to figure which hive support is relevant based on Hadoop distribution name
    String hadoopVersion = VersionInfo.getVersion();
    for (HiveSupport hiveSupport : HiveSupport.values()) {
      if (hiveSupport.getHadoopVersionPattern() != null &&
        hiveSupport.getHadoopVersionPattern().matcher(hadoopVersion).matches()) {
        return hiveSupport;
      }
    }

    boolean useLatestVersionForUnsupported = Constants.Explore.HIVE_AUTO_LATEST_VERSION.equals(
      cConf.get(Constants.Explore.HIVE_VERSION_RESOLUTION_STRATEGY));
    if (hadoopVersion.contains(CDH)) {
      if (useLatestVersionForUnsupported) {
        LOG.info("Hive distribution in CDH Hadoop version '{}' is not supported. " +
                   "Continuing with latest version of Hive module available.",
                 hadoopVersion);
        return HiveSupport.HIVE_CDH_LATEST;
      }
      throw new RuntimeException(String.format("Hive distribution in Hadoop version '%s' is not supported." +
                                                 " Set the configuration '%s' to false to start up without Explore. " +
                                                 "Or set the configuration '%s' to '%s' to use the latest " +
                                                 "version of Hive module available",
                                               hadoopVersion,
                                               Constants.Explore.EXPLORE_ENABLED,
                                               Constants.Explore.HIVE_VERSION_RESOLUTION_STRATEGY,
                                               Constants.Explore.HIVE_AUTO_LATEST_VERSION));
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
    } else if (hiveVersion.startsWith(("2.1"))) {
      return HiveSupport.HIVE_1_2;
    }

    if (useLatestVersionForUnsupported) {
      LOG.info("Hive distribution '{}' is not supported. Continuing with latest version of Hive module available.",
               hiveVersion);
      return HiveSupport.HIVE_LATEST;
    }

    throw new RuntimeException(String.format("Hive distribution '%s' is not supported." +
                                               " Set the configuration '%s' to false to start up without Explore. " +
                                               "Or set the configuration '%s' to '%s' to use the latest " +
                                               "Hive module available",
                                             hiveVersion,
                                             Constants.Explore.EXPLORE_ENABLED,
                                             Constants.Explore.HIVE_VERSION_RESOLUTION_STRATEGY,
                                             Constants.Explore.HIVE_AUTO_LATEST_VERSION));
  }

  /**
   * Patch hive classes by bytecode rewriting in the given source jar. Currently it rewrite the following classes:
   *
   * <ul>
   *   <li>
   *     {@link HiveAuthFactory} - This is for skipping kerberos authentication from the explore service container.
   *     {@link SessionState} - This is to workaround a native memory leakage bug due to
   *                            unclosed URLClassloaders, introduced by HIVE-14037. In normal Java process this
   *                            leakage won't be a problem as eventually those URLClassLoaders will get GC and
   *                            have the memory released. However, since explore container runs in YARN and YARN
   *                            monitor the RSS memory usage, it is highly possible that the URLClassLoader won't get
   *                            GC due to low heap memory usage, while already taken up all the allowed RSS memory.
   *                            We don't need aux jars added inside the explore JVM since all CDAP classes are already
   *                            in the classloader. We only use aux jars config to tell hive to localize CDAP jars
   *                            to task containers.
   *   </li>
   * </ul>
   *
   *
   * @param sourceJar the source jar to look for the {@link HiveAuthFactory} class.
   * @param targetJar the target jar to write to if rewrite happened
   * @return the source jar if there is no rewrite happened; the target jar if rewrite happened.
   * @throws IOException if failed to read/write to the jar files.
   */
  public static File patchHiveClasses(File sourceJar, File targetJar) throws IOException {
    try (JarFile input = new JarFile(sourceJar)) {
      // See if need to rewrite any classes from the jar
      boolean needPatch = false;
      for (String classFile : HIVE_CLASS_FILES_TO_PATCH.keySet()) {
        needPatch = needPatch || (input.getEntry(classFile) != null);
      }

      // If the given jar doesn't contain any class that needs to be patch, just return the source jar.
      if (!needPatch) {
        return sourceJar;
      }

      // Copy all the entries from the source jar and return the hive auth factory class during the process.
      try (JarOutputStream output = new JarOutputStream(new FileOutputStream(targetJar))) {
        Enumeration<JarEntry> sourceEntries = input.entries();
        while (sourceEntries.hasMoreElements()) {
          JarEntry entry = sourceEntries.nextElement();
          output.putNextEntry(new JarEntry(entry.getName()));

          try (InputStream entryInputStream = input.getInputStream(entry)) {
            Set<String> patchMethods = HIVE_CLASS_FILES_TO_PATCH.get(entry.getName());
            if (patchMethods == null) {
              ByteStreams.copy(entryInputStream, output);
              continue;
            }

            // Patch the class
            output.write(Classes.rewriteMethodToNoop(entry.getName(), entryInputStream, patchMethods));
          }
        }
      }

      return targetJar;
    }
  }

  // Determines whether the execution engine is spark, by checking the SessionConf (if provided) and then the HiveConf.
  public static boolean isSparkEngine(HiveConf hiveConf, @Nullable Map<String, String> sessionConf) {
    return "spark".equals(getEngine(hiveConf, sessionConf));
  }

  // Determines whether the execution engine is tez, by checking the SessionConf (if provided) and then the HiveConf.
  public static boolean isTezEngine(HiveConf hiveConf, @Nullable Map<String, String> sessionConf) {
    return "tez".equals(getEngine(hiveConf, sessionConf));
  }

  private static String getEngine(HiveConf hiveConf, @Nullable Map<String, String> sessionConf) {
    // sessionConf overrides hiveConf
    if (sessionConf != null && sessionConf.containsKey(HIVE_EXECUTION_ENGINE)) {
      return sessionConf.get(HIVE_EXECUTION_ENGINE);
    }
    return hiveConf.get(HIVE_EXECUTION_ENGINE);
  }
}
