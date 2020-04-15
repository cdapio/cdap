/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.ssh.DefaultSSHSession;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import joptsimple.OptionSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.utils.Dependencies;
import org.apache.twill.internal.utils.Resources;
import org.apache.twill.launcher.FindFreePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * A {@link TwillPreparer} implementation that uses ssh to launch a single {@link TwillRunnable}.
 */
class RemoteExecutionTwillPreparer extends AbstractRuntimeTwillPreparer {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionTwillPreparer.class);
  private static final String SETUP_SPARK_SH = "setupSpark.sh";
  private static final String SETUP_SPARK_PY = "setupSpark.py";
  private static final String SPARK_ENV_SH = "sparkEnv.sh";

  private final SSHConfig sshConfig;
  private final Location serviceProxySecretLocation;

  RemoteExecutionTwillPreparer(CConfiguration cConf, Configuration hConf,
                               SSHConfig sshConfig, @Nullable Location serviceProxySecretLocation,
                               TwillSpecification twillSpec, ProgramRunId programRunId, ProgramOptions programOptions,
                               LocationCache locationCache, LocationFactory locationFactory,
                               TwillControllerFactory controllerFactory) {
    super(cConf, hConf, twillSpec, programRunId, programOptions, locationCache, locationFactory, controllerFactory);
    this.sshConfig = sshConfig;
    this.serviceProxySecretLocation = serviceProxySecretLocation;
  }

  @Override
  protected void addLocalFiles(Path stagingDir, Map<String, LocalFile> localFiles) throws IOException {
    createLauncherJar(localFiles);
    createTwillJar(createBundler(stagingDir), localFiles);
  }

  @Override
  protected void addRuntimeConfigFiles(Path runtimeConfigDir) throws IOException {
    saveResource(runtimeConfigDir, SETUP_SPARK_SH);
    saveResource(runtimeConfigDir, SETUP_SPARK_PY);
  }

  @Override
  protected void launch(TwillRuntimeSpecification twillRuntimeSpec, RuntimeSpecification runtimeSpec,
                        JvmOptions jvmOptions, Map<String, String> environments, Map<String, LocalFile> localFiles,
                        TimeoutChecker timeoutChecker) throws Exception {
    try (SSHSession session = new DefaultSSHSession(sshConfig)) {
      String targetPath = session.executeAndWait("mkdir -p ./" + getProgramRunId().getRun(),
                                                 "echo `pwd`/" + getProgramRunId().getRun()).trim();
      // Upload files
      localizeFiles(session, localFiles, targetPath, runtimeSpec);

      // Upload service socks proxy secret
      localizeServiceProxySecret(session, targetPath);

      // Currently we only support one TwillRunnable
      String runnableName = runtimeSpec.getName();
      int memory = Resources.computeMaxHeapSize(runtimeSpec.getResourceSpecification().getMemorySize(),
                                                twillRuntimeSpec.getReservedMemory(runnableName),
                                                twillRuntimeSpec.getMinHeapRatio(runnableName));

      // Spark env setup script
      session.executeAndWait(String.format("bash %s/%s/%s %s/%s/%s > %s/%s",
                                           targetPath, Constants.Files.RUNTIME_CONFIG_JAR, SETUP_SPARK_SH,
                                           targetPath, Constants.Files.RUNTIME_CONFIG_JAR, SETUP_SPARK_PY,
                                           targetPath, SPARK_ENV_SH));
      // Generates the launch script
      byte[] scriptContent = generateLaunchScript(targetPath, runnableName,
                                                  memory, jvmOptions, environments).getBytes(StandardCharsets.UTF_8);
      //noinspection OctalInteger
      session.copy(new ByteArrayInputStream(scriptContent),
                   targetPath, "launcher.sh", scriptContent.length, 0755, null, null);

      timeoutChecker.throwIfTimeout();

      LOG.info("Starting runnable {} for runId {} with SSH", runnableName, getProgramRunId());
      session.executeAndWait(targetPath + "/launcher.sh");
    }
  }

  /**
   * Localize files to the target host.
   */
  private void localizeFiles(SSHSession session, Map<String, LocalFile> localFiles,
                             String targetPath, RuntimeSpecification runtimeSpec) throws IOException {

    // A map to remember what URI has already been uploaded to what target path.
    // This helps reducing the bandwidth when same file is uploaded to different target path.
    Map<URI, String> localizedFiles = new HashMap<>();
    String localizedDir = targetPath + "/.localized";
    session.executeAndWait("mkdir -p " + localizedDir);

    for (LocalFile localFile : Iterables.concat(localFiles.values(), runtimeSpec.getLocalFiles())) {
      URI uri = localFile.getURI();

      // If not yet uploaded, upload it
      String localizedFile = localizedFiles.get(uri);
      if (localizedFile == null) {
        String fileName = Hashing.md5().hashString(uri.toString()).toString() + "-" + getFileName(uri);
        localizedFile = localizedDir + "/" + fileName;
        try (InputStream inputStream = openURI(uri)) {
          LOG.debug("Upload file {} to {}@{}:{}", uri, session.getUsername(), session.getAddress(), localizedFile);
          //noinspection OctalInteger
          session.copy(inputStream, localizedDir, fileName, localFile.getSize(), 0644,
                       localFile.getLastModified(), localFile.getLastModified());
        }
        localizedFiles.put(uri, localizedFile);
      }

      // If it is an archive, expand it. If is a file, create a hardlink.
      if (localFile.isArchive()) {
        String expandedDir = targetPath + "/" + localFile.getName();
        LOG.debug("Expanding archive {} on host {} to {}",
                  localizedFile, session.getAddress().getHostName(), expandedDir);
        session.executeAndWait(
          "mkdir -p " + expandedDir,
          "cd " + expandedDir,
          String.format("jar xf %s", localizedFile)
        );
      } else {
        LOG.debug("Create hardlink {} on host {} to {}/{}",
                  localizedFile, session.getAddress().getHostName(), targetPath, localFile.getName());
        session.executeAndWait(String.format("ln %s %s/%s", localizedFile, targetPath, localFile.getName()));
      }
    }
  }

  private void createTwillJar(final ApplicationBundler bundler,
                              Map<String, LocalFile> localFiles) throws IOException {
    LOG.debug("Create and copy {}", Constants.Files.TWILL_JAR);
    Location location = getLocationCache().get(Constants.Files.TWILL_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        bundler.createBundle(targetLocation, ApplicationMasterMain.class, TwillContainerMain.class, OptionSpec.class);
      }
    });

    LOG.debug("Done {}", Constants.Files.TWILL_JAR);
    localFiles.put(Constants.Files.TWILL_JAR, createLocalFile(Constants.Files.TWILL_JAR, location, true));
  }

  /**
   * Creates the launcher.jar for launch the main application.
   */
  private void createLauncherJar(Map<String, LocalFile> localFiles) throws IOException {

    LOG.debug("Create and copy {}", Constants.Files.LAUNCHER_JAR);

    Location location = getLocationCache().get(Constants.Files.LAUNCHER_JAR, new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        // Create a jar file with the TwillLauncher and FindFreePort and dependent classes inside.
        try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
          ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
          if (classLoader == null) {
            classLoader = getClass().getClassLoader();
          }
          Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
            @Override
            public boolean accept(String className, URL classUrl, URL classPathUrl) {
              try {
                jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                try (InputStream is = classUrl.openStream()) {
                  ByteStreams.copy(is, jarOut);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return true;
            }
          }, RemoteLauncher.class.getName(), FindFreePort.class.getName());
        }
      }
    });

    LOG.debug("Done {}", Constants.Files.LAUNCHER_JAR);

    localFiles.put(Constants.Files.LAUNCHER_JAR, createLocalFile(Constants.Files.LAUNCHER_JAR, location, false));
  }

  /**
   * Finds a resource from the current {@link ClassLoader} and copy the content to the given directory.
   */
  private void saveResource(Path targetDir, String resourceName) throws IOException {
    URL url = getClassLoader().getResource(resourceName);
    if (url == null) {
      // This shouldn't happen.
      throw new IOException("Failed to find script " + resourceName + " in classpath");
    }

    try (InputStream is = url.openStream()) {
      Files.copy(is, targetDir.resolve(resourceName));
    }
  }

  /**
   * Returns the context ClassLoader if there is any, otherwise, returns ClassLoader of this class.
   */
  private ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return classLoader == null ? getClass().getClassLoader() : classLoader;
  }

  /**
   * Opens an {@link InputStream} that reads the content of the given {@link URI}.
   */
  private InputStream openURI(URI uri) throws IOException {
    String scheme = uri.getScheme();

    if (scheme == null || "file".equals(scheme)) {
      return new FileInputStream(uri.getPath());
    }

    // If having the same schema as the location factory, use the location factory to open the stream
    if (Objects.equals(getLocationFactory().getHomeLocation().toURI().getScheme(), scheme)) {
      return getLocationFactory().create(uri).getInputStream();
    }

    // Otherwise, fallback to using whatever supported in the JVM
    return uri.toURL().openStream();
  }

  /**
   * Returns the file name of a given {@link URI}. The file name is the last part of the path, separated by {@code /}.
   */
  private String getFileName(URI uri) {
    String path = uri.getPath();
    int idx = path.lastIndexOf('/');
    return idx >= 0 ? path.substring(idx + 1) : path;
  }

  /**
   * Generates the shell script for launching the JVM process of the runnable that will run on the remote host.
   */
  private String generateLaunchScript(String targetPath, String runnableName,
                                      int memory, JvmOptions jvmOptions, Map<String, String> environments) {
    String logsDir = targetPath + "/logs";

    StringWriter writer = new StringWriter();
    PrintWriter scriptWriter = new PrintWriter(writer, true);

    scriptWriter.println("#!/bin/bash");

    for (Map.Entry<String, String> env : environments.entrySet()) {
      scriptWriter.printf("export %s=\"%s\"\n", env.getKey(), env.getValue());
    }

    scriptWriter.printf("export %s=\"%s\"\n", EnvKeys.TWILL_RUNNABLE_NAME, runnableName);
    scriptWriter.printf("mkdir -p %s/tmp\n", targetPath);
    scriptWriter.printf("mkdir -p %s\n", logsDir);
    scriptWriter.printf("cd %s\n", targetPath);

    scriptWriter.printf("if [ -e %s/%s ]; then\n", targetPath, SPARK_ENV_SH);
    scriptWriter.printf("  source %s/%s\n", targetPath, SPARK_ENV_SH);
    scriptWriter.printf("fi\n");

    scriptWriter.println("export HADOOP_CLASSPATH=`hadoop classpath`");

    scriptWriter.printf(
      "nohup java -Djava.io.tmpdir=tmp -Dcdap.runid=%s -cp %s/%s -Xmx%dm %s %s '%s' true %s >%s/stdout 2>%s/stderr &\n",
      getProgramRunId().getRun(), targetPath, Constants.Files.LAUNCHER_JAR, memory,
      jvmOptions.getAMExtraOptions(),
      RemoteLauncher.class.getName(),
      RemoteExecutionJobMain.class.getName(),
      getProgramRunId().getRun(),
      logsDir, logsDir);

    scriptWriter.flush();

    // Expands the <LOG_DIR> placement holder to the log directory
    return writer.toString().replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR, logsDir);
  }

  /**
   * Localize key store files to the remote host.
   */
  private void localizeServiceProxySecret(SSHSession session, String targetPath) throws Exception {
    if (serviceProxySecretLocation == null) {
      return;
    }
    try (InputStream is = serviceProxySecretLocation.getInputStream()) {
      //noinspection OctalInteger
      session.copy(is, targetPath, io.cdap.cdap.common.conf.Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD_FILE,
                   serviceProxySecretLocation.length(), 0600, null, null);
    }
  }
}
