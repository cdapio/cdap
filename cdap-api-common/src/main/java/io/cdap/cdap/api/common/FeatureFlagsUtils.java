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
 *
 */

package io.cdap.cdap.api.common;

import java.util.HashMap;
import java.util.Map;

public class FeatureFlagsUtils {

  public static final String FEATURE_FLAG_PREFIX = "feature.";

  public static Map<String, String> extractFeatureFlags(Map<String, String> conf) {
    Map<String, String> featureFlags = new HashMap<>();
    for (String name : conf.keySet()) {
      if (name.startsWith(FEATURE_FLAG_PREFIX)) {
        String value = conf.get(name);
        if (!(("true".equals(value) || ("false".equals(value))))) {
          throw new IllegalArgumentException("Configured flag is not a valid boolean: name="
                                               + name + ", value=" + value);
        }
        featureFlags.put(name, value);
      }
    }
    return featureFlags;
  }

}
