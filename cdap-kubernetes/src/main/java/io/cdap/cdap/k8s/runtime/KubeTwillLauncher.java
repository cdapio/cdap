/*
 * Copyright © 2020-2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.master.environment.k8s.KubeMasterEnvironment;
import io.cdap.cdap.master.environment.k8s.PodInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Preconditions;
import io.kubernetes.client.util.Config;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.apache.twill.internal.utils.Instances;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A {@link MasterEnvironmentRunnable} for running {@link TwillRunnable} in the current process.
 */
public class KubeTwillLauncher implements MasterEnvironmentRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(KubeTwillLauncher.class);
  private static final Gson GSON = new Gson();

  private final MasterEnvironmentRunnableContext context;
  private final KubeMasterEnvironment masterEnv;

  private volatile boolean stopped;
  private TwillRunnable twillRunnable;

  public KubeTwillLauncher(MasterEnvironmentRunnableContext context, MasterEnvironment masterEnv) {
    this.context = context;
    if (!(masterEnv instanceof KubeMasterEnvironment)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Expected a KubeMasterEnvironment");
    }
    this.masterEnv = (KubeMasterEnvironment) masterEnv;
  }

  @Override
  public void run(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Requires runnable name in the argument");
    }
    String runnableName = args[0];
    Path runtimeConfigDir = Paths.get(Constants.Files.RUNTIME_CONFIG_JAR);
    Path argumentsPath = runtimeConfigDir.resolve(Constants.Files.ARGUMENTS);

    // Deserialize the arguments
    List<String> appArgs;
    List<String> runnableArgs;
    try (Reader reader = Files.newBufferedReader(argumentsPath, StandardCharsets.UTF_8)) {

      JsonObject jsonObj = GSON.fromJson(reader, JsonObject.class);
      appArgs = GSON.fromJson(jsonObj.get("arguments"), new TypeToken<List<String>>() { }.getType());
      Map<String, List<String>> map = GSON.fromJson(jsonObj.get("runnableArguments"),
                                                    new TypeToken<Map<String, List<String>>>() { }.getType());
      runnableArgs = map.getOrDefault(runnableName, Collections.emptyList());
    }

    Path applicationJarPath = runtimeConfigDir.resolve(Constants.Files.APPLICATION_JAR);
    Path destinationDir = Files.createTempDirectory(Paths.get("."), "unpacked");
    long classLoaderStart = System.currentTimeMillis();
    unJar(applicationJarPath.toFile(), destinationDir.toFile());
    URL[] jarURLs = recursiveGetJars(destinationDir).stream().toArray(URL[]::new);
    ClassLoader appClassLoader = new URLClassLoader(jarURLs, context.getClass().getClassLoader());
    LOG.debug("Created application classloader in {} ms.", System.currentTimeMillis() - classLoaderStart);

    PodInfo podInfo = masterEnv.getPodInfo();
    try {
      TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create()
        .fromJson(runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC).toFile());

      RuntimeSpecification runtimeSpec = twillRuntimeSpec.getTwillSpecification().getRunnables().get(runnableName);
      RunId runId = twillRuntimeSpec.getTwillAppRunId();

      String runnableClassName = runtimeSpec.getRunnableSpecification().getClassName();
      Class<?> runnableClass = appClassLoader.loadClass(runnableClassName);
      if (!TwillRunnable.class.isAssignableFrom(runnableClass)) {
        throw new IllegalArgumentException("Class " + runnableClass + " is not an instance of " + TwillRunnable.class);
      }

      twillRunnable = (TwillRunnable) Instances.newInstance(runnableClass);

      try (KubeTwillContext twillContext = new KubeTwillContext(runtimeSpec, runId,
                                                                RunIds.fromString(runId.getId() + "-0"),
                                                                appArgs.toArray(new String[0]),
                                                                runnableArgs.toArray(new String[0]), masterEnv)) {
        twillRunnable.initialize(twillContext);
        if (!stopped) {
          twillRunnable.run();
        }
      }
    } finally {
      try {
        TwillRunnable runnable = twillRunnable;
        if (runnable != null) {
          runnable.destroy();
        }
      } finally {
        if (Arrays.stream(args).noneMatch(str -> str.equalsIgnoreCase(KubeMasterEnvironment.DISABLE_POD_DELETION))) {
          // Delete the pod itself to avoid pod goes into CrashLoopBackoff. This is added for preview pods.
          // When pod is exited, exponential backoff happens. So pod restart time keep increasing.
          // Deleting pod does not trigger exponential backoff.
          // See https://github.com/kubernetes/kubernetes/issues/57291
          deletePod(podInfo);
        }
      }
    }
  }

  @Override
  public void stop() {
    stopped = true;
    TwillRunnable runnable = this.twillRunnable;
    if (runnable != null) {
      runnable.stop();
    }
  }

  private void deletePod(PodInfo podInfo) {
    try {
      ApiClient apiClient = Config.defaultClient();
      CoreV1Api api = new CoreV1Api(apiClient);
      V1DeleteOptions delOptions = new V1DeleteOptions()
        .preconditions(new V1Preconditions().uid(podInfo.getUid()))
        .gracePeriodSeconds(1L);
      api.deleteNamespacedPodAsync(podInfo.getName(), podInfo.getNamespace(), null,
                                   null, null, null, null, delOptions, new ApiCallbackAdapter<>());
    } catch (Exception e) {
      LOG.warn("Failed to delete pod {} with uid {}", podInfo.getName(), podInfo.getUid(), e);
    }
  }

  private static void unJar(File jarFile, File targetDirectory) throws IOException {
    try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(jarFile)))) {
      Path targetPath = targetDirectory.toPath();
      Files.createDirectories(targetPath);

      ZipEntry entry;
      while ((entry = zipIn.getNextEntry()) != null) {
        Path output = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(output);
        } else {
          Files.createDirectories(output.getParent());
          Files.copy(zipIn, output);
        }
      }
    }
  }

  private static List<URL> recursiveGetJars(Path targetDirectory) throws IOException {
    List<URL> jars = new ArrayList<>();
    for (File f: targetDirectory.toFile().listFiles()) {
      if (f.isFile() && f.toPath().toString().endsWith(".jar")) {
        jars.add(f.toURI().toURL());
      } else if (f.isDirectory()) {
        jars.addAll(recursiveGetJars(f.toPath().toAbsolutePath()));
      }
    }
    return jars;
  }
}
