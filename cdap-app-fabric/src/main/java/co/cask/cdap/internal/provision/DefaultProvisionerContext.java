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

package co.cask.cdap.internal.provision;

import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.SparkCompat;
import co.cask.cdap.runtime.spi.provisioner.ProgramRun;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.ssh.SSHContext;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context for a {@link Provisioner} extension
 */
public class DefaultProvisionerContext implements ProvisionerContext {
  private final ProgramRun programRun;
  private final Map<String, String> properties;
  private final SSHContext sshContext;
  private final SparkCompat sparkCompat;

  public DefaultProvisionerContext(ProgramRunId programRunId, Map<String, String> properties,
                                   SparkCompat sparkCompat, @Nullable SSHContext sshContext) {
    this.programRun = new ProgramRun(programRunId.getNamespace(), programRunId.getApplication(),
                                     programRunId.getProgram(), programRunId.getRun());
    this.properties = Collections.unmodifiableMap(properties);
    this.sshContext = sshContext;
    this.sparkCompat = sparkCompat;
  }

  @Override
  public ProgramRun getProgramRun() {
    return programRun;
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
  public SSHContext getSSHContext() {
    if (sshContext == null) {
      throw new UnsupportedOperationException("SSH is not supported");
    }
    return sshContext;
  }
}
