/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class contains common methods that are needed by DataprocProvisioner and DataprocRuntimeJobManager.
 */
public final class DataprocUtils {

  /**
   * Utilty class to parse the keyvalue string from UI Widget and return back HashMap.
   * String is of format  <key><keyValueDelimiter><value><delimiter><key><keyValueDelimiter><value>
   * eg:  networktag1=out2internet;networktag2=priority
   * The return from the method is a map with key value pairs of (networktag1 out2internet) and (networktag2 priority)
   *
   * @param configValue String to be parsed into key values format
   * @param delimiter Delimiter used for keyvalue pairs
   * @param keyValueDelimiter Delimiter between key and value.
   * @return Map of Key value pairs parsed from input configValue using the delimiters.
   */
  public static Map<String, String> parseKeyValueConfig(@Nullable String configValue, String delimiter,
                                                        String keyValueDelimiter) throws IllegalArgumentException {
    Map<String, String> map = new HashMap<>();
    if (configValue == null) {
      return map;
    }
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter, 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid KeyValue " + property);
      }
      String key = parts[0];
      String value = parts[1];
      map.put(key, value);
    }
    return map;
  }

  private DataprocUtils() {
    // no-op
  }
}
