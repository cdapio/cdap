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

import co.cask.cdap.messaging.store.MessageTable;

import java.util.Comparator;

/**
 * A {@link Comparator} for {@link MessageTable.Entry}.
 * The order is first by generation, followed by publish timestamp and sequence id.
 */
final class MessageTableEntryComparator implements Comparator<MessageTable.Entry> {

  @Override
  public int compare(MessageTable.Entry entry1, MessageTable.Entry entry2) {
    int cmp = Integer.compare(entry1.getGeneration(), entry2.getGeneration());
    if (cmp != 0) {
      return cmp;
    }
    cmp = Long.compare(entry1.getPublishTimestamp(), entry2.getPublishTimestamp());
    if (cmp != 0) {
      return cmp;
    }
    return Integer.compare(entry1.getSequenceId() & 0xFFFF, entry2.getSequenceId() & 0xFFFF);
  }
}
