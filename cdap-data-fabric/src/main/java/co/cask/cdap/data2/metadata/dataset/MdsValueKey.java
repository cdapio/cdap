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

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;

import javax.annotation.Nullable;

/**
 * Key used to store metadata values.
 */
public class MdsValueKey {
  private static final byte[] ROW_PREFIX = {'v'};

  public static String getMetadataKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [rowPrefix][targetType][targetId][metadata-type][key], so skip the first few strings.

    // Skip rowType
    keySplitter.skipBytes();

    // Skip targetType
    keySplitter.skipString();

    // Skip targetId
    if (type.equals(Id.Program.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Stream.View.class.getSimpleName())) {
      // skip namespace, stream, view
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Artifact.class.getSimpleName())) {
      // skip namespace, name, version
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }

    // Skip metadata-type
    keySplitter.skipString();

    return keySplitter.getString();
  }

  public static String getTargetType(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [rowPrefix][targetType][targetId][metadata-type][key]
    keySplitter.getBytes();
    return keySplitter.getString();
  }

  public static MDSKey getMDSKey(Id.NamespacedId targetId, @Nullable MetadataDataset.MetadataType type,
                                 @Nullable String key) {
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(ROW_PREFIX);
    builder.add(targetType);
    KeyHelper.addNamespaceIdToKey(builder, targetId);
    if (type != null) {
      builder.add(type.toString());
    }
    if (key != null) {
      builder.add(key);
    }

    return builder.build();
  }

  public static Id.NamespacedId getNamespaceIdFromKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();

    // The rowkey is [rowPrefix][targetType][targetId][metadata-type][key], so skip the first two.
    keySplitter.skipBytes();
    keySplitter.skipString();
    return KeyHelper.getNamespaceIdFromKey(keySplitter, type);
  }

  public static String getNamespaceId(MDSKey key) {
    MDSKey.Splitter keySplitter = key.split();

    // The rowkey is [rowPrefix][targetType][targetId][metadata-type][key], so skip the first two.
    keySplitter.skipBytes();
    keySplitter.skipString();
    // We are getting the first part of [targetId] which always be the namespace id.
    return keySplitter.getString();
  }
}
