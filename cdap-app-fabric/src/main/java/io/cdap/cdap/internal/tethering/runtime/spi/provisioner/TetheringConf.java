/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering.runtime.spi.provisioner;

import io.cdap.cdap.proto.id.EntityId;

import java.util.Map;

/**
 * Configuration for the Tethering Provisioner.
 */
public class TetheringConf {
  public static final String TETHERED_INSTANCE_PROPERTY = "tetheredInstanceName";
  public static final String TETHERED_NAMESPACE_PROPERTY = "tetheredNamespace";

  private final String tetheredInstanceName;
  private final String tetheredNamespace;

  private TetheringConf(String tetheredInstanceName, String tetheredNamespace) {
    this.tetheredInstanceName = tetheredInstanceName;
    this.tetheredNamespace = tetheredNamespace;
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  public static TetheringConf fromProperties(Map<String, String> properties) {
    String tetheredInstanceName = getString(properties, TETHERED_INSTANCE_PROPERTY);
    String tetheredNamespace = getString(properties, TETHERED_NAMESPACE_PROPERTY);
    EntityId.ensureValidNamespace(tetheredNamespace);
    EntityId.ensureValidId("tetheredInstanceName", tetheredInstanceName);
    return new TetheringConf(tetheredInstanceName, tetheredNamespace);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid tethering config. '%s' must be specified.", key));
    }
    return val;
  }

  public String getTetheredInstanceName() {
    return tetheredInstanceName;
  }

  public String getTetheredNamespace() {
    return tetheredNamespace;
  }
}
