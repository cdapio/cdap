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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.provisioner.ProgramRun;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerMetrics;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.LocationFactory;


public class MockProvisionerContext implements ProvisionerContext {

  private final Map<String, String> properties;
  private ProgramRunInfo programRunInfo;
  private SparkCompat sparkCompat;
  private VersionInfo appCDAPVersionInfo;
  private String cdapVersion;
  private String profileName;
  private ErrorCategory errorCategory;

  public MockProvisionerContext() {
    this(null);
  }

  public MockProvisionerContext(ProgramRunInfo programRunInfo) {
    this.properties = new HashMap<>();
    this.programRunInfo = programRunInfo;
  }

  @Override
  public ProgramRun getProgramRun() {
    return null;
  }

  @Override
  public ProgramRunInfo getProgramRunInfo() {
    return programRunInfo;
  }

  public void setProgramRunInfo(ProgramRunInfo programRunInfo) {
    this.programRunInfo = programRunInfo;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  public void addProperty(String key, String val) {
    properties.put(key, val);
  }

  public void clearProperties() {
    properties.clear();
  }

  @Nullable
  @Override
  public SSHContext getSSHContext() {
    return null;
  }

  @Override
  public SparkCompat getSparkCompat() {
    return sparkCompat;
  }

  public void setSparkCompat(SparkCompat sparkCompat) {
    this.sparkCompat = sparkCompat;
  }

  public void setErrorCategory(ErrorCategory errorCategory) {
    this.errorCategory = errorCategory;
  }

  @Override
  public String getCDAPVersion() {
    return cdapVersion;
  }

  public void setCdapVersion(String cdapVersion) {
    this.cdapVersion = cdapVersion;
  }

  @Override
  public VersionInfo getCDAPVersionInfo() {
    return null;
  }

  @Override @Nullable
  public VersionInfo getAppCDAPVersionInfo() {
    return appCDAPVersionInfo;
  }

  public void setAppCDAPVersionInfo(VersionInfo appCDAPVersionInfo) {
    this.appCDAPVersionInfo = appCDAPVersionInfo;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return null;
  }

  @Override
  public RuntimeMonitorType getRuntimeMonitorType() {
    return null;
  }

  @Override
  public String getProfileName() {
    return profileName;
  }

  public void setProfileName(String profileName) {
    this.profileName = profileName;
  }

  @Override
  public ProvisionerMetrics getMetrics(Map<String, String> context) {
    return new ProvisionerMetrics() {
      @Override
      public void count(String metricName, int delta) {}

      @Override
      public void gauge(String metricName, long value) {}
    };
  }

  @Override
  public <T> CompletionStage<T> execute(Callable<T> callable) {
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      result.complete(callable.call());
    } catch (Exception e) {
      result.completeExceptionally(e);
    }
    return result;
  }

  @Nullable
  @Override
  public ErrorCategory getErrorCategory() {
    return errorCategory;
  }
}
