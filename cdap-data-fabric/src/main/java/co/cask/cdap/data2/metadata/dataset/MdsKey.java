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

import co.cask.cdap.data2.dataset2.lib.table.EntityIdKeyHelper;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;

import javax.annotation.Nullable;

/**
 * Key used to store metadata values and indexes.
 */
final class MdsKey {
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

    // Skip targetId
    if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(ProgramId.class))) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(ApplicationId.class))) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(DatasetId.class))) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(StreamId.class))) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(StreamViewId.class))) {
      // skip namespace, stream, view
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(EntityIdKeyHelper.TYPE_MAP.get(ArtifactId.class))) {
      // skip namespace, name, version
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else {
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
  static MDSKey getMDSValueKey(NamespacedEntityId targetId, @Nullable String key) {
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
    MDSKey key = new MDSKey.Builder().add(MdsKey.VALUE_ROW_PREFIX).build();
    return key.getKey();
  }

  static byte[] getIndexRowPrefix() {
    MDSKey key = new MDSKey.Builder().add(MdsKey.INDEX_ROW_PREFIX).build();
    return key.getKey();
  }

  private static MDSKey.Builder getMDSKeyPrefix(NamespacedEntityId targetId, byte[] rowPrefix) {
    String targetType = EntityIdKeyHelper.getTargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(rowPrefix);
    builder.add(targetType);
    EntityIdKeyHelper.addTargetIdToKey(builder, targetId);
    return builder;
  }

  private MdsKey() {
  }
}
