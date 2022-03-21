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

package io.cdap.cdap.runtime.spi.provisioner;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;

/**
 * Context for provisioner operations.
 */
public interface ProvisionerContext {

  /**
   * @return the program run
   * @deprecated Use {@link #getProgramRunInfo()} instead
   */
  @Deprecated
  ProgramRun getProgramRun();

  /**
   * @return the program run information
   */
  ProgramRunInfo getProgramRunInfo();

  /**
   * Get the provisioner properties for this program run. These properties will start off as the provisioner properties
   * associated with the profile of the program run. The properties will then be overridden by any program preferences
   * that are prefixed with 'system.profile.properties.', with the prefixed stripped. Those properties will then be
   * overridden by any runtime arguments or schedule properties that are prefixed with 'system.profile.properties.',
   * with the prefixed stripped.
   *
   * @return the provisioner properties for the program run
   */
  Map<String, String> getProperties();

  /**
   * Returns the {@link SSHContext} for performing ssh operations. The context will be null if RunimeJobManager is
   * used to submit and monitor job.
   */
  @Nullable
  SSHContext getSSHContext();

  /**
   * Returns a {@link SparkCompat} object that represents the Spark version being used in CDAP.
   */
  SparkCompat getSparkCompat();

  /**
   * @return the CDAP version
   */
  @Deprecated
  default String getCDAPVersion() {
    return getCDAPVersionInfo().toString();
  }

  /**
   * @return the CDAP version information
   */
  VersionInfo getCDAPVersionInfo();

  /**
   *
   * @return version of CDAP pipeline was created or update with. Can be used to adjust default values for
   * backwards compatibility. If pipeline was created before this field was introduced,
   * null is returned.
   */
  @Nullable
  VersionInfo getAppCDAPVersionInfo();

  /**
   * Returns the {@link LocationFactory} used by the CDAP system.
   */
  LocationFactory getLocationFactory();

  /**
   * Returns the runtime monitor type for this provisioner.
   */
  RuntimeMonitorType getRuntimeMonitorType();

  /**
   * Returns a {@link ProvisionerMetrics} object configured based on {@code context}.
   */
  ProvisionerMetrics getMetrics(Map<String, String> context);

  /**
   * Executes the given {@link Callable} task asynchronously.
   *
   * @param callable the task to execute
   * @param <T> type of the result returned by the {@link Callable}
   * @return a {@link CompletionStage} that carries result of the task execution
   */
  <T> CompletionStage<T> execute(Callable<T> callable);

  /**
   * @return provisioner profile name.
   */
  @Nullable
  String getProfileName();
}
