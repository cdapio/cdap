/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.serialize;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility functions for serialization.
 */
final class LogSerializerUtil {

  private static final String MDC_NULL_KEY = ".null";

  private LogSerializerUtil() {}

  static String stringOrNull(Object obj) {
    return obj == null ? null : obj.toString();
  }

  static Map<String, String> encodeMDC(Map<String, String> mdc) {
    Map<String, String> encodeMap = new HashMap<>(mdc.size());
    for (Map.Entry<String, String> entry : mdc.entrySet()) {
      encodeMap.put(entry.getKey() == null ? MDC_NULL_KEY : entry.getKey(), entry.getValue());
    }
    return encodeMap;
  }

  static Map<String, String> decodeMDC(Map<?, ?> map) {
    if (map == null) {
      // Returns an empty map as ILoggingEvent guarantees MDC never null.
      return new HashMap<>();
    }

    Map<String, String> stringMap = new HashMap<>(map.size());
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      // AVRO does not allow null map keys.
      stringMap.put(entry.getKey() == null || entry.getKey().toString().equals(MDC_NULL_KEY) ?
                      null : entry.getKey().toString(),
                    entry.getValue() == null ? null : entry.getValue().toString());
    }
    return stringMap;
  }
}
