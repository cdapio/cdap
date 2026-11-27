/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.HashUtils;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.distributed.runtimejob.DefaultRuntimeJobInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.CacheableLocalFile;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TwillPreparer} implementation that uses runtime job manager to launch a single {@link
 * TwillRunnable}.
 */
class RuntimeJobTwillPreparer extends AbstractRuntimeTwillPreparer {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeJobTwillPreparer.class);

  private final Map<String, Location> secretFiles;
  private final Supplier<RuntimeJobManager> jobManagerSupplier;
  private final ProgramOptions programOptions;
  private final boolean artifactsComputeHash;
  private final boolean artifactsComputeHashSnapshot;
  private final int artifactsComputeHashTimeBucketDays;

  RuntimeJobTwillPreparer(
      CConfiguration cConf,
      Configuration hConf,
      TwillSpecification twillSpec,
      ProgramRunId programRunId,
      ProgramOptions programOptions,
      Map<String, Location> secretFiles,
      LocationCache locationCache,
      LocationFactory locationFactory,
      TwillControllerFactory controllerFactory,
      Supplier<RuntimeJobManager> jobManagerSupplier) {
    super(
        cConf,
        hConf,
        twillSpec,
        programRunId,
        programOptions,
        locationCache,
        locationFactory,
        controllerFactory);
    this.secretFiles = secretFiles;
    this.jobManagerSupplier = jobManagerSupplier;
    this.programOptions = programOptions;
    this.artifactsComputeHash =
        cConf.getBoolean(io.cdap.cdap.common.conf.Constants.AppFabric.ARTIFACTS_COMPUTE_HASH);
    this.artifactsComputeHashSnapshot =
        cConf.getBoolean(
            io.cdap.cdap.common.conf.Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_SNAPSHOT);
    this.artifactsComputeHashTimeBucketDays =
        cConf.getInt(
            io.cdap.cdap.common.conf.Constants.AppFabric.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS);
  }

  /**
   * Parses a jvm options string (e.g. {@code -XYZ -Dkey=value}) and extracts key/value properties
   * provided by the {@code -D} options.
   */
  @VisibleForTesting
  static Map<String, String> parseJvmProperties(String jvmOpts) {
    Map<String, String> jvmProperties = new LinkedHashMap<>();
    int idx = jvmOpts.indexOf("-D");
    while (idx >= 0) {
      int equalIdx = jvmOpts.indexOf('=', idx);
      if (equalIdx < 0) {
        throw new IllegalArgumentException(
            "Java properties must be in -Dkey=value format: " + jvmOpts);
      }
      String key = jvmOpts.substring(idx + 2, equalIdx);

      // Reached end of string, the value must be empty
      if (equalIdx + 1 == jvmOpts.length()) {
        jvmProperties.put(key, "");
        break;
      }
      int startIdx = equalIdx + 1;
      int endIdx;
      // Quoted value
      if (jvmOpts.charAt(startIdx) == '"') {
        startIdx++;
        endIdx = jvmOpts.indexOf('"', startIdx);
        if (endIdx < 0) {
          throw new IllegalArgumentException(
              "Missing end quote in Java property " + key + ": " + jvmOpts);
        }
      } else {
        endIdx = jvmOpts.indexOf(' ', equalIdx);
        if (endIdx < 0) {
          endIdx = jvmOpts.length();
        }
      }
      jvmProperties.put(key, jvmOpts.substring(startIdx, endIdx));
      idx = jvmOpts.indexOf("-D", endIdx);
    }
    return jvmProperties;
  }

  @Override
  protected void launch(
      TwillRuntimeSpecification twillRuntimeSpec,
      RuntimeSpecification runtimeSpec,
      JvmOptions jvmOptions,
      Map<String, String> environments,
      Map<String, LocalFile> localFiles,
      TimeoutChecker timeoutChecker)
      throws Exception {
    try (RuntimeJobManager jobManager = jobManagerSupplier.get()) {
      timeoutChecker.throwIfTimeout();
      Map<String, LocalFile> localizeFiles = new HashMap<>(localFiles);
      for (Map.Entry<String, Location> secretFile : secretFiles.entrySet()) {
        Location secretFileLocation = secretFile.getValue();
        localizeFiles.put(
            secretFile.getKey(),
            new DefaultLocalFile(
                secretFile.getKey(),
                secretFileLocation.toURI(),
                secretFileLocation.lastModified(),
                secretFileLocation.length(),
                false,
                null));
      }

      RuntimeJobInfo runtimeJobInfo =
          createRuntimeJobInfo(
              runtimeSpec,
              localizeFiles,
              jvmOptions.getRunnableExtraOptions(runtimeSpec.getName()));
      LOG.info(
          "Starting runnable {} for runId {} with job manager.",
          runtimeSpec.getName(),
          getProgramRunId());
      // launch job using job manager
      jobManager.launch(runtimeJobInfo);
    }
  }

  @Override
  String getApplicationJarLocalizedName(String hashVal) {
    ProjectInfo.Version cdapVersion = ProjectInfo.getVersion();
    String computedHashVal = hashVal;
    if (artifactsComputeHash && (artifactsComputeHashSnapshot || !cdapVersion.isSnapshot())) {
      if (artifactsComputeHashTimeBucketDays > 0) {
        computedHashVal =
            HashUtils.timeBucketHash(
                hashVal, artifactsComputeHashTimeBucketDays, System.currentTimeMillis());
      }
    }
    return String.format("application_%s.jar", computedHashVal);
  }

  private RuntimeJobInfo createRuntimeJobInfo(
      RuntimeSpecification runtimeSpec, Map<String, LocalFile> localFiles, String jvmOpts) {

    Map<String, String> jvmProperties = parseJvmProperties(jvmOpts);
    LOG.info("JVM properties {}", jvmProperties);
    Map<String, String> runtimeJobArguments = new HashMap<>();
    String applicationJarLocalizedName = localFiles.get(Constants.Files.APPLICATION_JAR).getName();
    // The applicationJarLocalizedName is passed in runtimeJobArguments will be used by
    // runtimeJobManager while launching the job so that it can be added in classpath in
    // runner cluster.
    runtimeJobArguments.put(Constants.Files.APPLICATION_JAR, applicationJarLocalizedName);

    Collection<? extends LocalFile> files =
        Stream.concat(localFiles.values().stream(), runtimeSpec.getLocalFiles().stream())
            .collect(Collectors.toList());

    Collection<LocalFile> resultingFiles = new ArrayList<>();

    Set<String> cacheableFiles;
    if (programOptions.getArguments().hasOption(ProgramOptionConstants.CACHEABLE_FILES)) {
      cacheableFiles =
          GSON.fromJson(
              programOptions.getArguments().getOption(ProgramOptionConstants.CACHEABLE_FILES),
              Set.class);
    } else {
      cacheableFiles = new HashSet<>();
    }
    // Add application jar to cacheable files to reuse it across launches.
    cacheableFiles.add(applicationJarLocalizedName);

    for (LocalFile f : files) {
      if (!cacheableFiles.contains(f.getName())) {
        resultingFiles.add(f);
      } else {
        resultingFiles.add(new CacheableLocalFile(f));
      }
    }

    ResourceSpecification resourceSpecification = runtimeSpec.getResourceSpecification();
    return new DefaultRuntimeJobInfo(
        getProgramRunId(), resultingFiles, jvmProperties, runtimeJobArguments,
        resourceSpecification.getVirtualCores(), resourceSpecification.getMemorySize());
  }
}
