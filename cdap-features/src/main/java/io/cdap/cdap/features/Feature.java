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

package io.cdap.cdap.features;

import io.cdap.cdap.api.FeatureFlagsProvider;
import io.cdap.cdap.api.PlatformInfo;
import io.cdap.cdap.api.common.FeatureFlagsUtil;

import java.util.Map;

/**
 * Defines Features Flags to be used in CDAP.
 * Features take the version that they were introduced as a first parameter. Optionally they can take a
 * second parameter to define their default behavior if they are not present in configuration.
 * By default, features default to enabled after they are introduced, and disabled before they were introduced
 */
public enum Feature {
  FEATURE_FLAGS_IN_RUNTIME("6.6.0");
  
  public static final String FEATURE_FLAG_PREFIX = FeatureFlagsUtil.FEATURE_FLAG_PREFIX;
  private final PlatformInfo.Version versionIntroduced;
  private final boolean defaultAfterIntroduction;
  private final String featureFlagString;

  Feature(String versionIntroduced) {
    this(versionIntroduced, true);
  }

  Feature(String versionDeployed, boolean defaultAfterIntroduction) {
    this.featureFlagString = FEATURE_FLAG_PREFIX + this.name().toLowerCase().replace('_', '.') + ".enabled";
    this.versionIntroduced = new PlatformInfo.Version(versionDeployed);
    this.defaultAfterIntroduction = defaultAfterIntroduction;
  }

  /**
   * Returns if the feature flag should be enabled.
   * First it checks featureFlagProvider to see if the feature flag has been defined.
   * If not defined then it uses when the feature flag was first introduced, if the platform version is
   * before or equal to when it was introduced it returns false, otherwise it returns defaultAfterIntroduction.
   *
   * @param featureFlagsProvider provides which feature flags have been set.
   * @return if the Feature Flag is enabled
   */
  public boolean isEnabled(FeatureFlagsProvider featureFlagsProvider) {
    return isEnabled(featureFlagsProvider.getFeatureFlags());
  }

  /**
   * Returns if the feature flag should be enabled.
   * First it checks configuration to see if the feature flag has been defined.
   * If not defined then it uses when the feature flag was first introduced, if the platform version is
   * before or equal to when it was introduced it returns false, otherwise it returns defaultAfterIntroduction.
   *
   * @param configuration provides which feature flags have been set.
   * @return if the Feature Flag is enabled
   */
  public boolean isEnabled(Map<String, String> configuration) {
    String featureFlagValue = configuration.get(featureFlagString);
    if (featureFlagValue == null) {
      return getDefaultValue();
    }
    return convertStringToBoolean(featureFlagValue);
  }

  /**
   *
   * @return string that identifies the feature flag.
   */
  public String getFeatureFlagString() {
    return featureFlagString;
  }

  private boolean convertStringToBoolean(String featureFlagValue) {
    return Boolean.parseBoolean(featureFlagValue);
  }

  private boolean getDefaultValue() {
    if (PlatformInfo.getVersion().compareTo(versionIntroduced) <= 0) {
      return false;
    } else {
      return defaultAfterIntroduction;
    }
  }

}
