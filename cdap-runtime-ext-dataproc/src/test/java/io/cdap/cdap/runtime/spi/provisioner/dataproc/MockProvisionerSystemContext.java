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

import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;

import java.util.HashMap;
import java.util.Map;

public class MockProvisionerSystemContext implements ProvisionerSystemContext {
  Map<String, String> properties;

  public MockProvisionerSystemContext() {
    properties = new HashMap<>();
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

  @Override
  public void reloadProperties() {
  }

  @Override
  public String getCDAPVersion() {
    return null;
  }
}
