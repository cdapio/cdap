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
 *
 */

package io.cdap.cdap.api.feature;

/**
 * Provides the ability to return feature flags
 */
public interface FeatureFlagsProvider {

  /**
   * This method tells is feature flag is currently enabled. To ensure proper feature life cycle
   * management and backwards compatibility please define features using {@link
   * io.cdap.cdap.features.Feature} enum and use {@link io.cdap.cdap.features.Feature#isEnabled}
   * method instead of directly calling this one
   *
   * @return value of the feature flag if set
   * @throws NullPointerException if feature flag name is not defined
   * @throws IllegalArgumentException if the feature flag is not a valid {@code boolean}
   */
  default boolean isFeatureEnabled(String name) {
    throw new UnsupportedOperationException();
  }
}
