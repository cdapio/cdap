/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.NamespacedEntityId;

/**
 * Key class to get v1 metadata history key information
 */
public final class MdsHistoryKey {
  private static final byte[] ROW_PREFIX = {'h'};

  public static MDSKey getMdsKey(NamespacedEntityId targetId, long time) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(ROW_PREFIX);
    EntityIdKeyHelper.addTargetIdToKey(builder, targetId);
    builder.add(invertTime(time));
    return builder.build();
  }

  /**
   * Extracts history time from the record
   * @param rowKey row key from which time needs to be extracted
   * @param type type of the entity
   * @return timestamp in the rowKey
   */
  static long extractTime(byte[] rowKey, String type) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // Skip rowType
    keySplitter.skipBytes();

    switch (type) {
      case MetadataEntity.PROGRAM:
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case MetadataEntity.APPLICATION:
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case MetadataEntity.DATASET:
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case MetadataEntity.STREAM:
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case MetadataEntity.VIEW:
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case MetadataEntity.ARTIFACT:
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      default:
        throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }

    return Long.MAX_VALUE - keySplitter.getLong();
  }

  static byte[] getHistoryRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(ROW_PREFIX).build();
    return key.getKey();
  }

  private static long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }

  private MdsHistoryKey() {
  }
}
