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

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception thrown when a feature is disabled.
 */
public class FeatureDisabledException extends Exception implements HttpErrorStatusProvider {
  /**
   * Represents disabled features
   */
  public enum Feature {
    AUTHENTICATION,
    AUTHORIZATION,
    EXPLORE
  }

  public static final String CDAP_SITE = "cdap-site.xml";

  private final Feature feature;
  private final String configName;
  private final String enableConfigKey;
  private final String enableConfigValue;

  public FeatureDisabledException(Feature feature, String configName, String enableConfigKey,
                                  String enableConfigValue) {
    super(String.format("Feature '%s' is not enabled. Please set '%s' to '%s' in the '%s' to enable this feature.",
                        feature.name().toLowerCase(), enableConfigKey, enableConfigValue, configName));
    this.feature = feature;
    this.configName = configName;
    this.enableConfigKey = enableConfigKey;
    this.enableConfigValue = enableConfigValue;
  }

  public Feature getFeature() {
    return feature;
  }

  public String getConfigName() {
    return configName;
  }

  public String getEnableConfigKey() {
    return enableConfigKey;
  }

  public String getEnableConfigValue() {
    return enableConfigValue;
  }

  @Override
  public int getStatusCode() {
    return HttpResponseStatus.NOT_IMPLEMENTED.getCode();
  }
}
