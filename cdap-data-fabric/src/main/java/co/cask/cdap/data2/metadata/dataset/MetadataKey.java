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
import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.nio.BufferUnderflowException;
import javax.annotation.Nullable;

/**
 * Key used to store Metadata values and indexes
 */
class MetadataKey {
  private static final byte[] VALUE_ROW_PREFIX = {'v'}; // value row prefix to store metadata value
  private static final byte[] INDEX_ROW_PREFIX = {'i'}; // index row prefix used for metadata search
  // if a target type cannot be determined because the MetadataEntity is an unknown CDAP entity then we will store it
  // as CUSTOM_TYPE.
  private static final String CUSTOM_TARGET_TYPE = "CUSTOM_TYPE";

  static String getMetadataKey(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows

    // Skip rowPrefix
    keySplitter.skipBytes();
    // Skip targetType
    keySplitter.skipString();

    // targetId are key-value par so always in set of two. For value row we will end up with only string in end ([key])
    // and for index row we will have two strings in end ([key][index]). In the first case trying to read second
    // string to lead BufferUnderFlowException which we catch and ignore returning the key.
    String key = null;
    while (keySplitter.hasRemaining()) {
      key = keySplitter.getString();
      try {
        keySplitter.skipString();
      } catch (BufferUnderflowException bfe) {
        break;
      }
    }
    return key;
  }

  static String getTargetType(byte[] rowKey) {
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
  static MDSKey getMDSValueKey(MetadataEntity metadataEntity, @Nullable String key) {
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
  static MDSKey getMDSIndexKey(MetadataEntity targetId, String key, @Nullable String index) {
    MDSKey.Builder builder = getMDSKeyPrefix(targetId, INDEX_ROW_PREFIX);
    builder.add(key);
    if (index != null) {
      builder.add(index);
    }
    return builder.build();
  }

  static MetadataEntity getMetadataEntityFromKey(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();

    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    // so skip the first two.
    keySplitter.skipBytes();
    keySplitter.skipString();
    return getTargetIdIdFromKey(keySplitter);
  }

  private static MetadataEntity getTargetIdIdFromKey(MDSKey.Splitter keySplitter) {
    MetadataEntity metadataEntity = new MetadataEntity();
    String key = keySplitter.getString();
    String value = keySplitter.getString();
    while (keySplitter.hasRemaining()) {
      // add the last read key and value in metadata entity and read the ones ahead for next loop
      // we do this since we don't want the last part as its metadata info ([key] or [key][index])
      metadataEntity = metadataEntity.append(key, value);
      key = keySplitter.getString();
      try {
        value = keySplitter.getString();
      } catch (BufferUnderflowException bfe) {
        // was index row and we exhausted our buffer
      }
    }
    return metadataEntity;
  }

  static byte[] getValueRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(MetadataKey.VALUE_ROW_PREFIX).build();
    return key.getKey();
  }

  static byte[] getIndexRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(MetadataKey.INDEX_ROW_PREFIX).build();
    return key.getKey();
  }

  private static MDSKey.Builder getMDSKeyPrefix(MetadataEntity metadataEntity, byte[] rowPrefix) {
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(rowPrefix);
    // Determine targetType for the known entities. For custom entities the type will be custom for now.
    String targetType;
    // Specifically convert and update because for some metadata entity we will not have all entity id parts and
    // this will ensure that during this phase we end up with all information. For example a MetadataEntity
    // containing application information might only have namespace and application and a missing version but the
    // EntityId representation of that include -SNAPSHOT as default version
    NamespacedEntityId namespacedEntityId = EntityId.fromMetadataEntity(metadataEntity);
    if (namespacedEntityId != null) {
      metadataEntity = namespacedEntityId.toMetadataEntity();
      targetType = EntityIdKeyHelper.getTargetType(namespacedEntityId);
    } else {
      targetType = CUSTOM_TARGET_TYPE;
    }
    builder.add(targetType);
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
