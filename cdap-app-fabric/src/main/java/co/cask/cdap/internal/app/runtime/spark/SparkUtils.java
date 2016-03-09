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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.asm.Methods;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A utility class to help determine Spark supports and locating Spark jar.
 */
public final class SparkUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SparkUtils.class);

  // Environment variable name for locating spark assembly jar file
  private static final String SPARK_ASSEMBLY_JAR = "SPARK_ASSEMBLY_JAR";
  // Environment variable name for locating spark home directory
  private static final String SPARK_HOME = "SPARK_HOME";

  // File name of the Spark conf directory as defined by the Spark framework
  // This is for the Hack to workaround CDAP-5019 (SPARK-13441)
  private static final String LOCALIZED_CONF_DIR = "__spark_conf__";
  private static final String LOCALIZED_CONF_DIR_ZIP = LOCALIZED_CONF_DIR + ".zip";
  // File entry name of the SparkConf properties file inside the Spark conf zip
  private static final String SPARK_CONF_FILE = "__spark_conf__.properties";

  private static final String SPARK_CLIENT_RESOURCE_NAME = "org/apache/spark/deploy/yarn/Client.class";

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
    // It's useful for overridding what being set for SPARK_HOME
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
   * Creates a new {@link URLClassLoader} that can load Spark classes. If Spark classes are already loadable
   * from the given parent ClassLoader, a new URLClassLoader will be created in a way such that it
   * always delegates to the parent for class loading.
   * Otherwise, it will try to find the Spark Assembly JAR to create a new URLClassLoader from it.
   *
   * @param parentClassLoader the parent ClassLoader for the new URLClassLoader created
   */
  public static URLClassLoader createSparkFrameworkClassLoader(ClassLoader parentClassLoader) {
    // Try to see if Spark class is already available in the CDAP system classpath.
    // It is for the Standalone case
    URL[] urls;

    try {
      parentClassLoader.loadClass("org.apache.spark.SparkConf");
      urls = new URL[0];
    } catch (ClassNotFoundException e) {
      // Try to locate Spark Assembly jar, which is for the distributed mode case
      try {
        urls = new URL[] { SparkUtils.locateSparkAssemblyJar().toURI().toURL() };
      } catch (IllegalStateException ex) {
        // Don't propagate as it's possible that a cluster doesn't have Spark configured
        // If someone deploy an artifact with Spark program inside, there will be NoClassDefFound exception and
        // will be handled by the ArtifactInspector.
        LOG.debug("Spark is not available");
        urls = new URL[0];
      } catch (MalformedURLException ex) {
        // This shouldn't happen
        throw Throwables.propagate(ex);
      }
    }
    return new URLClassLoader(urls, parentClassLoader);
  }

  /**
   * Prepares the resources that need to be localized to the Spark client container.
   *
   * @param cConf configuration for determining where is the CDAP data directory.
   * @param tempDir a temporary directory for file creation
   * @param localizeResources A map from localized name to {@link LocalizeResource} for this method to update
   * @return localized name of the Spark assembly jar file
   */
  public static String prepareSparkResources(CConfiguration cConf, File tempDir,
                                             Map<String, LocalizeResource> localizeResources) {
    File sparkAssemblyJar = locateSparkAssemblyJar();
    try {
      sparkAssemblyJar = getRewrittenSparkAssemblyJar(cConf);
    } catch (IOException e) {
      LOG.warn("Failed to locate the rewritten Spark Assembly JAR. Fallback to use the original jar.", e);
    }
    localizeResources.put(sparkAssemblyJar.getName(), new LocalizeResource(sparkAssemblyJar));

    // Shallow copy all files under directory defined by $HADOOP_CONF_DIR
    // If $HADOOP_CONF_DIR is not defined, use the location of "yarn-site.xml" to determine the directory
    // This is part of workaround for CDAP-5019 (SPARK-13441).
    File hadoopConfDir = null;
    if (System.getenv().containsKey(ApplicationConstants.Environment.HADOOP_CONF_DIR.key())) {
      hadoopConfDir = new File(System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.key()));
    } else {
      URL yarnSiteLocation = SparkUtils.class.getClassLoader().getResource("yarn-site.xml");
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

    return sparkAssemblyJar.getName();
  }

  /**
   * Creates a {@code __spark_conf__.zip} in the local directory by zipping contains from the
   * {@code __spark_conf__} directory, with the addition of the serialization of the Spark properties.
   * This method works in conjunction with the {@link #prepareSparkResources(CConfiguration, File, Map)} method and
   * should only be called from the the Spark client container process.
   *
   * @param sparkProperties the Spark properties to serialize
   */
  public static void createSparkConfZip(final Properties sparkProperties) {
    try {
      BundleJarUtil.createArchive(new File(LOCALIZED_CONF_DIR), new OutputSupplier<ZipOutputStream>() {
        @Override
        public ZipOutputStream getOutput() throws IOException {
          ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(LOCALIZED_CONF_DIR_ZIP));
          try {
            zipOutput.putNextEntry(new ZipEntry(SPARK_CONF_FILE));
            sparkProperties.store(zipOutput, "Spark configuration.");
            return zipOutput;
          } catch (IOException e) {
            zipOutput.close();
            throw e;
          }
        }
      });
    } catch (IOException e) {
      // Don't propagate the exception.
      // It is possible that the LOCALIZED_CONF_DIR doesn't exist if the prepareSparkResources
      // cannot locate the HADOOP_CONF_DIR (should be very rare, if not impossible).
      // In that case, will just let the SparkSubmit method handle it
      // (depending on the Spark version, some can handle it).
      LOG.warn("Failed to create {} file", LOCALIZED_CONF_DIR_ZIP, e);
    }
  }

  /**
   * Returns the Spark assembly jar file with the Spark Yarn Client rewritten. It is for workaround the bug in
   * CDAP-5019 (SPARK-13441).
   *
   * @param cConf configuration for determining where is the CDAP data directory.
   * @return the rewritten Spark assembly JAR file
   * @throws IOException if failed to create the rewritten jar
   * @throws IllegalStateException if failed to locate the original Spark assembly JAR file
   */
  public static synchronized File getRewrittenSparkAssemblyJar(CConfiguration cConf) throws IOException {
    File assemblyJar = locateSparkAssemblyJar();
    File tempDir = getTempDir(cConf);
    long vmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
    File rewrittenJar = new File(tempDir, vmStartTime + "-" + assemblyJar.getName());
    if (rewrittenJar.exists()) {
      return rewrittenJar;
    }

    File tempFile = File.createTempFile(rewrittenJar.getName(), ".tmp", tempDir);
    try {
      try (JarInputStream jarInput = new JarInputStream(new BufferedInputStream(new FileInputStream(assemblyJar)))) {
        try (
          JarOutputStream jarOutput = new JarOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)),
                                                          jarInput.getManifest())
        ) {
          // Use a larger and reusing the bytes make the copying slightly faster than ByteStreams.copy()
          byte[] buffer = new byte[65536];
          JarEntry jarEntry;
          while ((jarEntry = jarInput.getNextJarEntry()) != null) {
            if (JarFile.MANIFEST_NAME.equals(jarEntry.getName())) {
              continue;
            }

            JarEntry newEntry = new JarEntry(jarEntry.getName());
            jarOutput.putNextEntry(newEntry);

            try {
              if (jarEntry.isDirectory()) {
                continue;
              }

              if (SPARK_CLIENT_RESOURCE_NAME.equals(jarEntry.getName())) {
                jarOutput.write(rewriteSparkYarnClient(jarInput));
              } else {
                int len = jarInput.read(buffer);
                while (len >= 0) {
                  jarOutput.write(buffer, 0, len);
                  len = jarInput.read(buffer);
                }
              }
            } finally {
              jarOutput.closeEntry();
            }
          }
          if (!tempFile.renameTo(rewrittenJar)) {
            throw new IOException("Failed to rename " + tempFile + " to " + rewrittenJar);
          }
          return rewrittenJar;
        }
      }
    } finally {
      tempFile.delete();
    }
  }

  /**
   * Rewrites the bytecode of the org.apache.spark.deploy.yarn.Client class to fix the bug in SPARK-13441
   *
   * @param input {@link InputStream} for reading the original bytecode of the Client class.
   * @return The rewritten bytecode
   * @throws IOException if failed to read from the given input
   */
  private static byte[] rewriteSparkYarnClient(InputStream input) throws IOException {
    ClassReader cr = new ClassReader(input);
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cr.accept(new ClassVisitor(Opcodes.ASM5, cw) {
      @Override
      public MethodVisitor visitMethod(final int access, final String name,
                                       final String desc, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

        // Only rewrite the createConfArchive method
        if (!"createConfArchive".equals(name)) {
          return mv;
        }

        // Generate the method body to just return the hardcode LOCALIZED_CONF_DIR_ZIP file name
        if (Type.getReturnType(desc).equals(Type.getType(File.class))) {
          // Spark 1.5+ return type is File
          // This is what gets generated
          // return new File(LOCALIZED_CONF_DIR_ZIP);
          GeneratorAdapter mg = new GeneratorAdapter(mv, access, name, desc);
          mg.newInstance(Type.getType(File.class));
          mg.dup();
          mg.visitLdcInsn(LOCALIZED_CONF_DIR_ZIP);
          mg.invokeConstructor(Type.getType(File.class), Methods.getMethod(void.class, "<init>", String.class));
          mg.returnValue();
          mg.endMethod();
          return null;
        } else if (Type.getReturnType(desc).equals(Type.getType(Option.class))) {
          // Spark 1.4 return type is Option<File>
          // This is what gets generated
          // return Option.apply(new File(LOCALIZED_CONF_DIR_ZIP);
          GeneratorAdapter mg = new GeneratorAdapter(mv, access, name, desc);
          mg.newInstance(Type.getType(File.class));
          mg.dup();
          mg.visitLdcInsn(LOCALIZED_CONF_DIR_ZIP);
          mg.invokeConstructor(Type.getType(File.class), Methods.getMethod(void.class, "<init>", String.class));
          mg.invokeStatic(Type.getType(Option.class), Methods.getMethod(Option.class, "apply", Object.class));
          mg.checkCast(Type.getType(Option.class));
          mg.returnValue();
          mg.endMethod();
          return null;
        }
        return mv;
      }
    }, 0);
    return cw.toByteArray();
  }

  /**
   * Returns the local temporary directory as specified by the configuration.
   */
  private static File getTempDir(CConfiguration cConf) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    tempDir.mkdirs();
    return tempDir;
  }

  private SparkUtils() {
  }
}
