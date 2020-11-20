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

package io.cdap.cdap.internal.provision;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.provisioner.ProgramRun;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerMetrics;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Context for a {@link Provisioner} extension
 */
public class DefaultProvisionerContext implements ProvisionerContext {

  private final ProgramRun programRun;
  private final ProgramRunInfo programRunInfo;
  private final Map<String, String> properties;
  private final SSHContext sshContext;
  private final SparkCompat sparkCompat;
  private final String cdapVersion;
  private final LocationFactory locationFactory;
  private final RuntimeMonitorType runtimeMonitorType;
  private final MetricsCollectionService metricsCollectionService;
  private final String provisionerName;
  private final Executor executor;

  DefaultProvisionerContext(ProgramRunId programRunId, String provisionerName, Map<String, String> properties,
                            SparkCompat sparkCompat, @Nullable SSHContext sshContext, LocationFactory locationFactory,
                            RuntimeMonitorType runtimeMonitorType, MetricsCollectionService metricsCollectionService,
                            Executor executor) {
    this.programRun = new ProgramRun(programRunId.getNamespace(), programRunId.getApplication(),
                                     programRunId.getProgram(), programRunId.getRun());
    this.programRunInfo = new ProgramRunInfo.Builder()
      .setNamespace(programRunId.getNamespace())
      .setApplication(programRunId.getApplication())
      .setVersion(programRunId.getVersion())
      .setProgramType(programRunId.getType().name())
      .setProgram(programRunId.getProgram())
      .setRun(programRunId.getRun())
      .build();
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.sshContext = sshContext;
    this.sparkCompat = sparkCompat;
    this.locationFactory = locationFactory;
    this.cdapVersion = ProjectInfo.getVersion().toString();
    this.runtimeMonitorType = runtimeMonitorType;
    this.metricsCollectionService = metricsCollectionService;
    this.provisionerName = provisionerName;
    this.executor = executor;
  }

  @Override
  public ProgramRun getProgramRun() {
    return programRun;
  }

  @Override
  public ProgramRunInfo getProgramRunInfo() {
    return programRunInfo;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public SparkCompat getSparkCompat() {
    return sparkCompat;
  }

  @Override
  @Nullable
  public SSHContext getSSHContext() {
    return sshContext;
  }

  @Override
  public String getCDAPVersion() {
    return cdapVersion;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return locationFactory;
  }

  @Override
  public RuntimeMonitorType getRuntimeMonitorType() {
    return runtimeMonitorType;
  }

  @Override
  public ProvisionerMetrics getMetrics(Map<String, String> context) {
    Map<String, String> tags = new HashMap<>(context);
    tags.put(Constants.Metrics.Tag.NAMESPACE, programRunInfo.getNamespace());
    tags.put(Constants.Metrics.Tag.RUN_ID, programRunInfo.getRun());
    tags.put(Constants.Metrics.Tag.PROGRAM, programRunInfo.getProgram());
    tags.put(Constants.Metrics.Tag.APP, programRunInfo.getApplication());
    tags.put(Constants.Metrics.Tag.PROVISIONER, provisionerName);
    return new DefaultProvisionerMetrics(metricsCollectionService.getContext(tags));
  }

  @Override
  public <T> CompletionStage<T> execute(Callable<T> callable) {
    CompletableFuture<T> result = new CompletableFuture<>();

    executor.execute(() -> {
      try {
        result.complete(callable.call());
      } catch (Throwable t) {
        result.completeExceptionally(t);
      }
    });
    return result;
  }
}
