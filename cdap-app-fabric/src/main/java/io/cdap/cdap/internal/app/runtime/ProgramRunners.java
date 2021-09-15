/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.joran.util.ConfigurationWatchListUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.annotation.Nullable;

/**
 * Utility class to provide common functionality that shares among different {@link ProgramRunner}.
 */
public final class ProgramRunners {

  /**
   * Impersonates as the given user to start a guava service
   *
   * @param user user to impersonate
   * @param service guava service start start
   */
  public static void startAsUser(String user, final Service service) throws IOException, InterruptedException {
    runAsUser(user, new Callable<ListenableFuture<Service.State>>() {
      @Override
      public ListenableFuture<Service.State> call() throws Exception {
        return service.start();
      }
    });
  }

  /**
   * Impersonates as the given user to perform an action.
   *
   * @param user user to impersonate
   * @param callable action to perform
   */
  public static <T> T runAsUser(String user, final Callable<T> callable) throws IOException, InterruptedException {
    return UserGroupInformation.createRemoteUser(user)
      .doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
  }

  /**
   * Updates the given arguments to always have the logical start time set.
   *
   * @param arguments the runtime arguments
   * @return the logical start time
   */
  public static long updateLogicalStartTime(Map<String, String> arguments) {
    String value = arguments.get(ProgramOptionConstants.LOGICAL_START_TIME);
    try {
      // value is only empty/null in in some unit tests
      long logicalStartTime = Strings.isNullOrEmpty(value) ? System.currentTimeMillis() : Long.parseLong(value);
      arguments.put(ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(logicalStartTime));
      return logicalStartTime;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(String.format(
        "%s is set to an invalid value %s. Please ensure it is a timestamp in milliseconds.",
        ProgramOptionConstants.LOGICAL_START_TIME, value));
    }
  }

  /**
   * Returns the {@link RunId} stored inside the given {@link ProgramOptions#getArguments()}.
   *
   * @throws IllegalArgumentException if the given options doesn't contain run id.
   */
  public static RunId getRunId(ProgramOptions programOptions) {
    String id = programOptions.getArguments().getOption(ProgramOptionConstants.RUN_ID);
    Preconditions.checkArgument(id != null, "Missing " + ProgramOptionConstants.RUN_ID + " in program options");
    return RunIds.fromString(id);
  }

  /**
   * Returns the application principal if there is one.
   *
   * @param programOptions the program options to extract information from
   * @return the application principal or {@code null} if no application principal is available.
   */
  @Nullable
  public static KerberosPrincipalId getApplicationPrincipal(ProgramOptions programOptions) {
    Arguments systemArgs = programOptions.getArguments();
    boolean hasAppPrincipal = Boolean.parseBoolean(systemArgs.getOption(ProgramOptionConstants.APP_PRINCIPAL_EXISTS));
    return hasAppPrincipal ? new KerberosPrincipalId(systemArgs.getOption(ProgramOptionConstants.PRINCIPAL)) : null;
  }

  /**
   * Same as {@link #createLogbackJar(Location)} except this method uses local {@link File} instead.
   */
  @Nullable
  public static File createLogbackJar(File targetFile) throws IOException {
    Location logbackJar = createLogbackJar(Locations.toLocation(targetFile));
    return logbackJar == null ? null : new File(logbackJar.toURI());
  }

  /**
   * Creates a jar that contains a logback.xml configured for the current process
   *
   * @param targetLocation the jar location
   * @return the {@link Location} where the jar was created to or {@code null} if "logback.xml" is not found
   *         in the current ClassLoader.
   * @throws IOException if failed in reading the logback xml or writing out the jar
   */
  @Nullable
  public static Location createLogbackJar(Location targetLocation) throws IOException {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof Context)) {
      return null;
    }

    URL logbackURL = ConfigurationWatchListUtil.getMainWatchURL((Context) loggerFactory);
    if (logbackURL == null) {
      return null;
    }

    try (InputStream input = logbackURL.openStream()) {
      try (JarOutputStream output = new JarOutputStream(targetLocation.getOutputStream())) {
        output.putNextEntry(new JarEntry("logback.xml"));
        ByteStreams.copy(input, output);
      }
      return targetLocation;
    }
  }

  /**
   * Returns the {@link ArtifactId} stored inside the given {@link ProgramOptions#getArguments()}.
   */
  public static ArtifactId getArtifactId(ProgramOptions programOptions) {
    String id = programOptions.getArguments().getOption(ProgramOptionConstants.ARTIFACT_ID);
    Preconditions.checkArgument(id != null, "Missing " + ProgramOptionConstants.ARTIFACT_ID + " in program options");
    return ArtifactId.fromIdParts(Splitter.on(':').split(id));
  }

  /**
   * Returns the {@link ClusterMode} stored inside the given {@link ProgramOptions#getArguments()}.
   */
  public static ClusterMode getClusterMode(ProgramOptions programOptions) {
    String clusterMode = programOptions.getArguments().getOption(ProgramOptionConstants.CLUSTER_MODE);

    // Default to ON_PREMISE for backward compatibility.
    return clusterMode == null ? ClusterMode.ON_PREMISE : ClusterMode.valueOf(clusterMode);
  }

  /**
   * Create a {@link MetricsContext} for emitting program metrics.
   * @param programRunId the {@link ProgramRunId} of the current execution
   * @param metricsTags a set of extra tags to be used for creating the {@link MetricsContext}
   * @param metricsCollectionService the underlying service for metrics publishing or {@code null} to suppress metrics
   *                                 publishing
   * @return a {@link MetricsContext} for emitting metrics for the current program context.
   */
  public static MetricsContext createProgramMetricsContext(ProgramRunId programRunId,
                                                            Map<String, String> metricsTags,
                                                            @Nullable MetricsCollectionService
                                                             metricsCollectionService) {
    Map<String, String> tags = Maps.newHashMap(metricsTags);
    tags.put(Constants.Metrics.Tag.NAMESPACE, programRunId.getNamespace());
    tags.put(Constants.Metrics.Tag.APP, programRunId.getApplication());
    tags.put(ProgramTypeMetricTag.getTagName(programRunId.getType()), programRunId.getProgram());
    tags.put(Constants.Metrics.Tag.RUN_ID, programRunId.getRun());
    return metricsCollectionService == null ? new NoopMetricsContext(tags) : metricsCollectionService.getContext(tags);
  }

  private ProgramRunners() {
    // no-op
  }
}
