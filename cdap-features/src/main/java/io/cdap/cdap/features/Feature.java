/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.api.PlatformInfo;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;

/**
 * Defines Features Flags to be used in CDAP. Features take the version that they were introduced as
 * a first parameter. Optionally they can take a second parameter to define their default behavior
 * if they are not present in configuration. By default, features default to enabled after they are
 * introduced, and disabled before they were introduced
 */
public enum Feature {
  REPLICATION_TRANSFORMATIONS("6.6.0"),
  EVENT_PUBLISH("6.7.0", false),
  EVENT_READER("6.10.0", false),
  PIPELINE_COMPOSITE_TRIGGERS("6.8.0"),
  PUSHDOWN_TRANSFORMATION_GROUPBY("6.7.0"),
  PUSHDOWN_TRANSFORMATION_DEDUPLICATE("6.7.0"),
  STREAMING_PIPELINE_CHECKPOINT_DELETION("6.7.1"),
  LIFECYCLE_MANAGEMENT_EDIT("6.8.0"),
  WRANGLER_FAIL_PIPELINE_FOR_ERROR("6.8.0"),
  STREAMING_PIPELINE_NATIVE_STATE_TRACKING("6.8.0", false),
  PUSHDOWN_TRANSFORMATION_WINDOWAGGREGATION("6.9.1"),
  SOURCE_CONTROL_MANAGEMENT_GIT("6.9.0"),
  SOURCE_CONTROL_MANAGEMENT_MULTI_APP("6.10.0"),
  WRANGLER_PRECONDITION_SQL("6.9.1"),
  WRANGLER_EXECUTION_SQL("6.10.0"),
  WRANGLER_SCHEMA_MANAGEMENT("6.10.0"),
  NAMESPACED_SERVICE_ACCOUNTS("6.10.0"),
  WRANGLER_KRYO_SERIALIZATION("6.10.1");

  private final PlatformInfo.Version versionIntroduced;
  private final boolean defaultAfterIntroduction;
  private final String featureFlagString;

  Feature(String versionIntroduced) {
    this(versionIntroduced, true);
  }

  Feature(String versionIntroduced, boolean defaultAfterIntroduction) {
    this.featureFlagString = this.name().toLowerCase().replace('_', '.') + ".enabled";
    this.versionIntroduced = new PlatformInfo.Version(versionIntroduced);
    this.defaultAfterIntroduction = defaultAfterIntroduction;
  }

  /**
   * Returns if the feature flag should be enabled. First it checks featureFlagProvider to see if
   * the feature flag has been defined. If not defined then it uses when the feature flag was first
   * introduced, if the platform version is before or equal to when it was introduced it returns
   * false, otherwise it returns defaultAfterIntroduction.
   *
   * @param featureFlagsProvider provides which feature flags have been set.
   * @return if the Feature Flag is enabled
   */
  public boolean isEnabled(FeatureFlagsProvider featureFlagsProvider) {
    try {
      return featureFlagsProvider.isFeatureEnabled(getFeatureFlagString());
    } catch (NullPointerException e) {
      return getDefaultValue();
    }
  }

  /**
   * Retrieve the string that identifies the feature flag.
   *
   * @return feature flag string
   */
  public String getFeatureFlagString() {
    return featureFlagString;
  }

  private boolean getDefaultValue() {
    if (PlatformInfo.getVersion().compareTo(versionIntroduced) <= 0) {
      return false;
    }
    return defaultAfterIntroduction;
  }

}

