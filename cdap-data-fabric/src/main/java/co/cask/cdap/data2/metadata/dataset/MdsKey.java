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

import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.NamespacedEntityId;

import javax.annotation.Nullable;

import static co.cask.cdap.api.metadata.MetadataEntity.APPLICATION;
import static co.cask.cdap.api.metadata.MetadataEntity.ARTIFACT;
import static co.cask.cdap.api.metadata.MetadataEntity.DATASET;
import static co.cask.cdap.api.metadata.MetadataEntity.PROGRAM;
import static co.cask.cdap.api.metadata.MetadataEntity.STREAM;
import static co.cask.cdap.api.metadata.MetadataEntity.V1_DATASET_INSTANCE;
import static co.cask.cdap.api.metadata.MetadataEntity.V1_VIEW;
import static co.cask.cdap.api.metadata.MetadataEntity.VIEW;

/**
 * Key class to get v1 metadata key information
 */
public final class MdsKey {
  private static final byte[] VALUE_ROW_PREFIX = {'v'}; // value row prefix to store metadata value
  private static final byte[] INDEX_ROW_PREFIX = {'i'}; // index row prefix used for metadata search

  static String getMetadataKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    // so skip the first few strings.

    // Skip rowType
    keySplitter.skipBytes();

    // Skip targetType
    keySplitter.skipString();
    type = type.toLowerCase();
    switch (type) {
      case PROGRAM:
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case APPLICATION:
      case DATASET:
      case V1_DATASET_INSTANCE:
      case STREAM:
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      case VIEW:
      case V1_VIEW:
      case ARTIFACT:
        keySplitter.skipString();
        keySplitter.skipString();
        keySplitter.skipString();
        break;
      default:
        throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }

    return keySplitter.getString();
  }

  static String getTargetType(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    keySplitter.getBytes();
    return keySplitter.getString();
  }

  /**
   * Creates a key for metadata value row in the format:
   * [{@link #VALUE_ROW_PREFIX}][targetType][targetId][key] for value index rows
   */
  public static MDSKey getMDSValueKey(NamespacedEntityId targetId, @Nullable String key) {
    MDSKey.Builder builder = getMDSKeyPrefix(targetId, VALUE_ROW_PREFIX);
    if (key != null) {
      builder.add(key);
    }
    return builder.build();
  }

  /**
   * Creates a key for metadata index row in the format:
   * [{@link #INDEX_ROW_PREFIX}][targetType][targetId][key][index] for value index rows
   */
  static MDSKey getMDSIndexKey(NamespacedEntityId targetId, String key, @Nullable String index) {
    MDSKey.Builder builder = getMDSKeyPrefix(targetId, INDEX_ROW_PREFIX);
    builder.add(key);
    if (index != null) {
      builder.add(index);
    }
    return builder.build();
  }

  static NamespacedEntityId getNamespacedIdFromKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();

    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    // so skip the first two.
    keySplitter.skipBytes();
    keySplitter.skipString();
    return EntityIdKeyHelper.getTargetIdIdFromKey(keySplitter, type);
  }

  static String getNamespaceId(MDSKey key) {
    MDSKey.Splitter keySplitter = key.split();

    // The rowkey is
    // [rowPrefix][targetType][targetId][key] for value rows and
    // [rowPrefix][targetType][targetId][key][index] for value index rows
    // so skip the first two.
    keySplitter.skipBytes();
    keySplitter.skipString();
    // We are getting the first part of [targetId] which always be the namespace id.
    return keySplitter.getString();
  }

  static byte[] getValueRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(VALUE_ROW_PREFIX).build();
    return key.getKey();
  }

  static byte[] getIndexRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(INDEX_ROW_PREFIX).build();
    return key.getKey();
  }

  private static MDSKey.Builder getMDSKeyPrefix(NamespacedEntityId targetId, byte[] rowPrefix) {
    String targetType = EntityIdKeyHelper.getV1TargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(rowPrefix);
    builder.add(targetType);
    EntityIdKeyHelper.addTargetIdToKey(builder, targetId);
    return builder;
  }

  private MdsKey() {
  }

}
