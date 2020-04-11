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

import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.provisioner.ProgramRun;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

  DefaultProvisionerContext(ProgramRunId programRunId, Map<String, String> properties,
                            SparkCompat sparkCompat, @Nullable SSHContext sshContext, LocationFactory locationFactory) {
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
}
