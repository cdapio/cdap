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
import co.cask.cdap.explore.service.hive.Hive12CDH5ExploreService;
import co.cask.cdap.explore.service.hive.Hive12ExploreService;
import co.cask.cdap.explore.service.hive.Hive13ExploreService;
import co.cask.cdap.explore.service.hive.Hive14ExploreService;
import co.cask.cdap.hive.ExploreUtils;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.service.auth.HiveAuthFactory;
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
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import javax.annotation.Nullable;

/**
 * Utility class for the explore service.
 */
public class ExploreServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreServiceUtils.class);

  private static final String HIVE_AUTHFACTORY_CLASS_NAME = "org.apache.hive.service.auth.HiveAuthFactory";
  private static final String HIVE_EXECUTION_ENGINE = "hive.execution.engine";

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

  public static Class<? extends ExploreService> getHiveService() {
    HiveSupport hiveVersion = checkHiveSupport(null);
    return hiveVersion.getHiveExploreServiceClass();
  }

  static boolean shouldEscapeColumns(Configuration hConf) {
    // backtick support was added in Hive13.
    ExploreServiceUtils.HiveSupport hiveSupport = checkHiveSupport(hConf.getClassLoader());
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
   * Rewrite the {@link HiveAuthFactory} class in the given source jar. The rewrite is for skipping kerberos
   * authentication from the explore service container.
   *
   * @param sourceJar the source jar to look for the {@link HiveAuthFactory} class.
   * @param targetJar the target jar to write to if rewrite happened
   * @return the source jar if there is no rewrite happened; the target jar if rewrite happened.
   * @throws IOException if failed to read/write to the jar files.
   */
  public static File rewriteHiveAuthFactory(File sourceJar, File targetJar) throws IOException {
    try (JarFile input = new JarFile(sourceJar)) {
      String hiveAuthFactoryPath = HIVE_AUTHFACTORY_CLASS_NAME.replace('.', '/') + ".class";
      ZipEntry hiveAuthFactoryEntry = input.getEntry(hiveAuthFactoryPath);

      // If the given jar doesn't contain the hive auth factory class, just return the source jar.
      if (hiveAuthFactoryEntry == null) {
        return sourceJar;
      }

      // Copy all the entries from the source jar and return the hive auth factory class during the process.
      try (JarOutputStream output = new JarOutputStream(new FileOutputStream(targetJar))) {
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
                  return new MethodVisitor(Opcodes.ASM5) {
                  };
                }
              }, 0);
              output.write(cw.toByteArray());
            } catch (Exception e) {
              throw new IOException("Unable to generate HiveAuthFactory class", e);
            }
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
