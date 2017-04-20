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

/**
 * A {@link MessageCache.Weigher} for the {@link MessageTable.Entry}.
 */
final class MessageTableEntryWeigher implements MessageCache.Weigher<MessageTable.Entry> {

  @Override
  public int weight(MessageTable.Entry entry) {
    // Some fixed overhead for the primitive and reference fields
    int weight = 40;
    byte[] payload = entry.getPayload();
    weight += payload == null ? 0 : payload.length;
    return weight;
  }
}
