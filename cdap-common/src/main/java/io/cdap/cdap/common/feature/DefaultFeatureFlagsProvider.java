/*
 * Copyright © 2014-2022 Cask Data, Inc.
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
package io.cdap.cdap.common.feature;

import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.conf.CConfiguration;

/**
 * Implementation of {@link FeatureFlagsProvider} using CConf to return FeatureFlag state
 */
public class DefaultFeatureFlagsProvider implements FeatureFlagsProvider {

  private static final String FEATURE_FLAG_PREFIX = "feature.";

  private final CConfiguration cConf;

  public DefaultFeatureFlagsProvider(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    String featureFlagName = FEATURE_FLAG_PREFIX + name;
    return cConf.getBoolean(featureFlagName);
  }
}
