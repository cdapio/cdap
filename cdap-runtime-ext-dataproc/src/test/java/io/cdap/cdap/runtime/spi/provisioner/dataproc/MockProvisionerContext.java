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

import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.provisioner.ProgramRun;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import org.apache.twill.filesystem.LocationFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


public class MockProvisionerContext implements ProvisionerContext {
  private Map<String, String> properties;

  public MockProvisionerContext() {
    properties = new HashMap<>();
  }

  @Override
  public ProgramRun getProgramRun() {
    return null;
  }

  @Override
  public ProgramRunInfo getProgramRunInfo() {
    return null;
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
    return null;
  }

  @Override
  public String getCDAPVersion() {
    return null;
  }

  @Override
  public LocationFactory getLocationFactory() {
    return null;
  }
}
