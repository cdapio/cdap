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

package co.cask.cdap.messaging.store.cache;

import co.cask.cdap.messaging.cache.MessageCache;
import co.cask.cdap.messaging.store.MessageTable;
import co.cask.cdap.proto.id.TopicId;

import javax.annotation.Nullable;

/**
 * A provider to provide {@link MessageCache} over {@link MessageTable.Entry} based on {@link TopicId}.
 */
public interface MessageTableCacheProvider {

  /**
   * Returns a {@link MessageCache} for the given topic.
   *
   * @param topicId the topic id
   * @return a {@link MessageCache} or {@code null} if caching is not enabled for the given topic.
   */
  @Nullable
  MessageCache<MessageTable.Entry> getMessageCache(TopicId topicId);
}
