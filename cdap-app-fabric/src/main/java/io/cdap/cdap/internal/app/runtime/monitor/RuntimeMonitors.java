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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.Maps;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class for runtime monitor.
 */
public final class RuntimeMonitors {

  /**
   * Creates a map from topic configuration name to the actual TMS topic based on the list of topic configuration names
   * specified by the {@link Constants.RuntimeMonitor#TOPICS_CONFIGS} key.
   */
  public static Map<String, String> createTopicConfigs(CConfiguration cConf) {
    return cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS).stream().flatMap(key -> {
      int idx = key.lastIndexOf(':');
      if (idx < 0) {
        return Stream.of(Maps.immutableEntry(key, cConf.get(key)));
      }

      try {
        int totalTopicCount = Integer.parseInt(key.substring(idx + 1));
        if (totalTopicCount <= 0) {
          throw new IllegalArgumentException("Total topic number must be positive for system topic config '" +
                                               key + "'.");
        }
        // For metrics, We make an assumption that number of metrics topics on runtime are not different than
        // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
        // keep track of different offsets for each metrics topic.
        // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
        String topicPrefix = key.substring(0, idx);
        return IntStream
          .range(0, totalTopicCount)
          .mapToObj(i -> Maps.immutableEntry(topicPrefix + ":" + i, cConf.get(topicPrefix) + i));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Total topic number must be a positive number for system topic config'"
                                             + key + "'.", e);
      }
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private RuntimeMonitors() {
    // no-op
  }
}
