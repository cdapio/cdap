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

package io.cdap.cdap.runtime.spi.provisioner.tether;

import java.util.Map;

/**
 * Configuration for the Tether provisioner.
 */
public class TetherConf {
  private final String tetheredInstanceName;
  private final String tetheredNamespace;

  private TetherConf(String tetheredInstanceName, String tetheredNamespace) {
    this.tetheredInstanceName = tetheredInstanceName;
    this.tetheredNamespace = tetheredNamespace;
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  public static TetherConf fromProperties(Map<String, String> properties) {
    String tetheredInstanceName = getString(properties, "tetheredInstanceName");
    String tetheredNamespace = getString(properties, "tetheredNamespace");
    return new TetherConf(tetheredInstanceName, tetheredNamespace);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
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
