/*
 * Copyright 2018 Cask Data, Inc.
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
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;

import javax.annotation.Nullable;

/**
 * Key used to store Metadata values and indexes
 */
class MetadataKey {
  private static final byte[] VALUE_ROW_PREFIX = {'v'}; // value row prefix to store metadata value
  private static final byte[] INDEX_ROW_PREFIX = {'i'}; // index row prefix used for metadata search
  private static final byte[] VALUE_ROW_PREFIX_KEY =
    new MDSKey.Builder().add(MetadataKey.VALUE_ROW_PREFIX).build().getKey();
  private static final byte[] INDEX_ROW_PREFIX_KEY =
    new MDSKey.Builder().add(MetadataKey.INDEX_ROW_PREFIX).build().getKey();


  static String extractMetadataKey(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows

    // Skip rowPrefix
    keySplitter.skipBytes();
    // Skip targetType
    keySplitter.skipString();

    // targetId are key-value par so always in set of two. For value row we will end up with only string in end ([key])
    // and for index row we will have two strings in end ([key][index]).
    String key = null;
    while (keySplitter.hasRemaining()) {
      key = keySplitter.getString();
      if (keySplitter.hasRemaining()) {
        keySplitter.skipString();
      } else {
        break;
      }
    }
    return key;
  }

  static String extractTargetType(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // skip rowPrefix
    keySplitter.skipBytes();
    // return targetType
    return keySplitter.getString();
  }

  /**
   * Creates a key for metadata value row in the format:
   * [{@link #VALUE_ROW_PREFIX}][targetType][targetId][key] for value index rows
   */
  static MDSKey createValueRowKey(MetadataEntity metadataEntity, @Nullable String key) {
    MDSKey.Builder builder = getMDSKeyPrefix(metadataEntity, VALUE_ROW_PREFIX);
    if (key != null) {
      builder.add(key);
    }
    return builder.build();
  }

  /**
   * Creates a key for metadata index row in the format:
   * [{@link #INDEX_ROW_PREFIX}][targetType][targetId][key][index] for value index rows
   */
  static MDSKey createIndexRowKey(MetadataEntity targetId, String key, @Nullable String index) {
    MDSKey.Builder builder = getMDSKeyPrefix(targetId, INDEX_ROW_PREFIX);
    builder.add(key);
    // index will be null for delete calls
    if (index != null) {
      builder.add(index);
    }
    return builder.build();
  }

  static MetadataEntity extractMetadataEntityFromKey(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();

    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    // so skip the first
    keySplitter.skipBytes();
    return getTargetIdIdFromKey(keySplitter);
  }

  private static MetadataEntity getTargetIdIdFromKey(MDSKey.Splitter keySplitter) {
    // get the type
    String targetType = keySplitter.getString();
    MetadataEntity metadataEntity = new MetadataEntity();
    String key = keySplitter.getString();
    String value = keySplitter.getString();
    while (keySplitter.hasRemaining()) {
      // add the last read key and value in metadata entity and read the ones ahead for next loop
      // we do this since we don't want the last part as its metadata info ([key] or [key][index])
      metadataEntity = metadataEntity.append(key, value);
      key = keySplitter.getString();
      if (keySplitter.hasRemaining()) {
        value = keySplitter.getString();
      } else {
        break;
      }
    }
    return metadataEntity.changeType(targetType);
  }

  static byte[] getValueRowPrefix() {
    return VALUE_ROW_PREFIX_KEY;
  }

  static byte[] getIndexRowPrefix() {
    return INDEX_ROW_PREFIX_KEY;
  }

  private static MDSKey.Builder getMDSKeyPrefix(MetadataEntity metadataEntity, byte[] rowPrefix) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(rowPrefix);
    builder.add(metadataEntity.getType());
    // add all the key value pairs from the metadata entity this is the targetId
    for (MetadataEntity.KeyValue keyValue : metadataEntity) {
      builder.add(keyValue.getKey());
      builder.add(keyValue.getValue());
    }
    return builder;
  }
  private MetadataKey() {
  }
}
