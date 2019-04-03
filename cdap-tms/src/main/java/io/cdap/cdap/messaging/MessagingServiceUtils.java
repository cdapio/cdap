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

package co.cask.cdap.messaging;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A util class for TMS.
 */
public final class MessagingServiceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingServiceUtils.class);

  /**
   * Returns a set of system {@link TopicId} as configured by the {@link Constants.MessagingSystem#SYSTEM_TOPICS}
   * property.
   *
   * @param cConf the configuration to get the system topics
   * @param ignoreInvalidTopic if {@code true}, invalid topics will be ignored; otherwise exception will be raised
   * @return a set of valid system {@link TopicId}.
   * @throws IllegalArgumentException if {@code ignoreInvalidTopic} is {@code true} and there is invalid topic being
   *                                  configured
   */
  public static Set<TopicId> getSystemTopics(CConfiguration cConf, boolean ignoreInvalidTopic) {
    Set<TopicId> systemTopics = new LinkedHashSet<>();

    for (String topic : cConf.getTrimmedStringCollection(Constants.MessagingSystem.SYSTEM_TOPICS)) {
      int idx = topic.lastIndexOf(':');
      if (idx < 0) {
        try {
          systemTopics.add(NamespaceId.SYSTEM.topic(topic));
        } catch (IllegalArgumentException e) {
          if (ignoreInvalidTopic) {
            LOG.warn("Ignore system topic '{}'. Reason: {}", topic, e.getMessage());
          } else {
            throw e;
          }
        }
        continue;
      }

      // If the current topic matches the format <common.prefix>:<total.topic.number>, expands it to
      // <total.topic.number> topics with the prefix <common.prefix> with numerical suffices
      // ranging from 0 to (<total.topic.number> - 1)
      try {
        int totalTopicCount = Integer.parseInt(topic.substring(idx + 1));
        if (totalTopicCount <= 0) {
          if (ignoreInvalidTopic) {
            LOG.warn("Ignore system topic '{}' because the total topic number {} in it is not positive integer.",
                     topic, totalTopicCount);
            continue;
          } else {
            throw new IllegalArgumentException("Total topic number must be positive for system topic '" + topic + "'.");
          }
        }

        // Add it to a list first before adding to the result set to avoid any potential partial result.
        List<TopicId> topics = new ArrayList<>(totalTopicCount);
        String topicPrefix = topic.substring(0, idx);
        for (int i = 0; i < totalTopicCount; i++) {
          topics.add(NamespaceId.SYSTEM.topic(topicPrefix + i));
        }
        systemTopics.addAll(topics);
      } catch (NumberFormatException e) {
        if (ignoreInvalidTopic) {
          LOG.warn("Ignore invalid system topic '{}' because of the invalid total topic number in it.", topic);
        } else {
          throw new IllegalArgumentException("Total topic number must be a positive number for system topic '"
                                               + topic + "'.");
        }
      } catch (IllegalArgumentException e) {
        if (ignoreInvalidTopic) {
          LOG.warn("Ignore system topic '{}'. Reason: {}", topic, e.getMessage());
        } else {
          throw e;
        }
      }
    }

    return Collections.unmodifiableSet(systemTopics);
  }


  private MessagingServiceUtils() {
    // no-op
  }
}
