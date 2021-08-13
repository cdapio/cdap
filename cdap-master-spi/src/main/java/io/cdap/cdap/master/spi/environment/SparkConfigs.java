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

package io.cdap.cdap.master.spi.environment;

import java.util.Map;

/**
 * Spark configs.
 */
public class SparkConfigs {
  private final Map<String, String> configs;
  private final String podTemplateString;
  private final String masterBasePath;

  public SparkConfigs(Map<String, String> configs, String masterBasePath, String podTemplateString) {
    this.configs = configs;
    this.masterBasePath = masterBasePath;
    this.podTemplateString = podTemplateString;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  public String getMasterBasePath() {
    return masterBasePath;
  }

  public String getPodTemplateString() {
    return podTemplateString;
  }
}
