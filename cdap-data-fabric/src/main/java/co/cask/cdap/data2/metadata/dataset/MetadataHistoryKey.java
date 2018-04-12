/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;

/**
 * Key used to store metadata history.
 */
class MetadataHistoryKey {
  private static final byte[] ROW_PREFIX = {'h'};

  static MDSKey getMetadataKey(MetadataEntity targetId, long time) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(ROW_PREFIX);
    for (MetadataEntity.KeyValue keyValue : targetId) {
      builder.add(keyValue.getKey());
      builder.add(keyValue.getValue());
    }
    builder.add(invertTime(time));
    return builder.build();
  }

  static MDSKey getMetdatdaScanStartKey(MetadataEntity metadataEntity, long time) {
    return getMetadataKey(metadataEntity, time);
  }

  static MDSKey getMetadataScanEndKey(MetadataEntity metadataEntity) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(ROW_PREFIX);
    for (MetadataEntity.KeyValue keyValue : metadataEntity) {
      builder.add(keyValue.getKey());
      builder.add(keyValue.getValue());
    }
    byte[] key = builder.build().getKey();
    return new MDSKey(Bytes.stopKeyForPrefix(key));
  }

  private static long invertTime(long time) {
    return Long.MAX_VALUE - time;
  }
}
