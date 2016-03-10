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

package co.cask.cdap.common;

/**
 * Exception thrown when a feature is disabled.
 */
public class FeatureDisabledException extends Exception {
  private final String feature;
  private final String configFile;
  private final String enableConfigKey;
  private final String enableConfigValue;

  public FeatureDisabledException(String feature, String configFile, String enableConfigKey, String enableConfigValue) {
    super(String.format("Feature '%s' is not enabled. Please set '%s' to '%s' in the config file '%s' to " +
                          "enable this feature.", feature, enableConfigKey, enableConfigValue, configFile));
    this.feature = feature;
    this.configFile = configFile;
    this.enableConfigKey = enableConfigKey;
    this.enableConfigValue = enableConfigValue;
  }

  public String getFeature() {
    return feature;
  }

  public String getConfigFile() {
    return configFile;
  }

  public String getEnableConfigKey() {
    return enableConfigKey;
  }

  public String getEnableConfigValue() {
    return enableConfigValue;
  }
}
