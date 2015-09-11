/*
 * Copyright 2015 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetadataSearchTargetType;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Dataset that manages Business Metadata using an {@link IndexedTable}.
 */
public class BusinessMetadataDataset extends AbstractDataset {
  private static final String TAGS_KEY = "tags";
  private static final String TAGS_SEPARATOR = ",";

  // column keys
  static final String KEYVALUE_COLUMN = "kv";
  static final String VALUE_COLUMN = "v";

  /**
   * Identifies the type of metadata - property or tag
   */
  private static enum MetadataType {
    PROPERTY("p"),
    TAG("t");

    private final String serializedForm;

    private MetadataType(String serializedForm) {
      this.serializedForm = serializedForm;
    }

    @Override
    public String toString() {
      return serializedForm;
    }
  }

  public static final String KEYVALUE_SEPARATOR = ":";

  private final IndexedTable indexedTable;

  public BusinessMetadataDataset(IndexedTable indexedTable) {
    super("businessMetadataIndexedTable", indexedTable);
    this.indexedTable = indexedTable;
  }

  /**
   * Add new business metadata.
   *
   * @param metadataRecord The value of the metadata to be saved.
   * @param metadataType {@link MetadataType} indicating the type of metadata - property or tag
   */
  private void setBusinessMetadata(BusinessMetadataRecord metadataRecord, MetadataType metadataType) {
    Id.NamespacedId targetId = metadataRecord.getTargetId();
    String key = metadataRecord.getKey();
    MDSKey mdsKey = getMDSKey(targetId, metadataType, key);

    // Put to the default column.
    write(mdsKey, metadataRecord);
  }

  /**
   * Sets a business metadata property for the specified {@link Id.NamespacedId}.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public void setProperty(Id.NamespacedId targetId, String key, String value) {
    setBusinessMetadata(new BusinessMetadataRecord(targetId, key, value), MetadataType.PROPERTY);
  }

  /**
   * Replaces existing tags of the specified {@link Id.NamespacedId} with a new set of tags.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param tags the tags to set
   */
  private void setTags(Id.NamespacedId targetId, String ... tags) {
    BusinessMetadataRecord tagsRecord =
      new BusinessMetadataRecord(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(tags));
    setBusinessMetadata(tagsRecord, MetadataType.TAG);
  }

  /**
   * Adds a new tag for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream).
   * @param tagsToAdd the tags to add
   */
  public void addTags(Id.NamespacedId targetId, String ... tagsToAdd) {
    Set<String> existingTags = getTags(targetId);
    Iterable<String> newTags = Iterables.concat(existingTags, Arrays.asList(tagsToAdd));
    BusinessMetadataRecord newTagsRecord =
      new BusinessMetadataRecord(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(newTags));
    setBusinessMetadata(newTagsRecord, MetadataType.TAG);
  }

  /**
   * Return business metadata based on type, target id, and key.
   *
   * @param targetId The id of the target
   * @param metadataType {@link MetadataType} indicating the type of metadata to retrieve - property or tag
   * @param key The metadata key to get
   * @return instance of {@link BusinessMetadataRecord} for the target type, id, and key
   */
  @Nullable
  private BusinessMetadataRecord getBusinessMetadata(Id.NamespacedId targetId, MetadataType metadataType, String key) {
    MDSKey mdsKey = getMDSKey(targetId, metadataType, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(VALUE_COLUMN);

    return new BusinessMetadataRecord(targetId, key, Bytes.toString(value));
  }

  /**
   * Retrieve the {@link BusinessMetadataRecord} corresponding to the specified key for the {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which the {@link BusinessMetadataRecord} is to be retrieved
   * @param key the property key for which the {@link BusinessMetadataRecord} is to be retrieved
   * @return the {@link BusinessMetadataRecord} corresponding to the specified key for the {@link Id.NamespacedId}
   */
  @Nullable
  public BusinessMetadataRecord getProperty(Id.NamespacedId targetId, String key) {
    return getBusinessMetadata(targetId, MetadataType.PROPERTY, key);
  }

  /**
   * Retrieves the business metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the specified {@link Id.NamespacedId}
   * @param metadataType {@link MetadataType} indicating the type of metadata to retrieve - property or tag
   * @return a Map representing the metadata for the specified {@link Id.NamespacedId}
   */
  private Map<String, String> getBusinessMetadata(Id.NamespacedId targetId, MetadataType metadataType) {
    String targetType = getTargetType(targetId);
    MDSKey mdsKey = getMDSKey(targetId, metadataType, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    Scanner scan = indexedTable.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String key = getMetadataKey(targetType, next.getRow());
        byte[] value = next.get(VALUE_COLUMN);
        if (key == null || value == null) {
          continue;
        }
        metadata.put(key, Bytes.toString(value));
      }
      return metadata;
    } finally {
      scan.close();
    }
  }

  /**
   * Retrieves all the properties for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which properties are to be retrieved
   * @return the properties of the specified {@link Id.NamespacedId}
   */
  public Map<String, String> getProperties(Id.NamespacedId targetId) {
    return getBusinessMetadata(targetId, MetadataType.PROPERTY);
  }

  /**
   * Retrieves all the tags for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which tags are to be retrieved
   * @return the tags of the specified {@link Id.NamespacedId}
   */
  public Set<String> getTags(Id.NamespacedId targetId) {
    BusinessMetadataRecord tags = getBusinessMetadata(targetId, MetadataType.TAG, TAGS_KEY);
    if (tags == null) {
      return new HashSet<>();
    }
    return Sets.newHashSet(Splitter.on(TAGS_SEPARATOR).omitEmptyStrings().trimResults().split(tags.getValue()));
  }

  /**
   * Removes all business metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which metadata is to be removed
   * @param metadataType {@link MetadataType} indicating the type of metadata to remove - property or tag
   */
  private void removeMetadata(Id.NamespacedId targetId, MetadataType metadataType) {
    removeMetadata(targetId, metadataType, Predicates.<String>alwaysTrue());
  }

  /**
   * Removes the specified keys from the business metadata of the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which the specified metadata keys are to be removed
   * @param metadataType {@link MetadataType} indicating the type of metadata to remove - property or tag
   * @param keys the keys to remove from the metadata of the specified {@link Id.NamespacedId}
   */
  private void removeMetadata(Id.NamespacedId targetId, MetadataType metadataType, String ... keys) {
    final Set<String> keySet = Sets.newHashSet(keys);
    removeMetadata(targetId, metadataType, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return keySet.contains(input);
      }
    });
  }

  /**
   * Removes all keys that satisfy a given predicate from the metadata of the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which keys are to be removed
   * @param metadataType {@link MetadataType} indicating the type of metadata to remove - property or tag
   * @param filter the {@link Predicate} that should be satisfied to remove a key
   */
  private void removeMetadata(Id.NamespacedId targetId, MetadataType metadataType, Predicate<String> filter) {
    String targetType = getTargetType(targetId);
    MDSKey mdsKey = getMDSKey(targetId, metadataType, null);
    byte[] prefix = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    Scanner scan = indexedTable.scan(prefix, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String keyValue = next.getString(KEYVALUE_COLUMN);
        String value = next.getString(VALUE_COLUMN);
        if (keyValue == null && value == null) {
          continue;
        }
        if (filter.apply(getMetadataKey(targetType, next.getRow()))) {
          indexedTable.delete(new Delete(next.getRow()));
        }
      }
    } finally {
      scan.close();
    }
  }

  /**
   * Removes the specified keys from the metadata properties of an entity.
   *
   * @param targetId the {@link Id.NamespacedId} from which to remove the specified keys
   * @param keys the keys to remove
   */
  public void removeProperties(Id.NamespacedId targetId, String ... keys) {
    removeMetadata(targetId, MetadataType.PROPERTY, keys);
  }

  /**
   * Removes the specified tags from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} from which to remove the specified tags
   * @param tagsToRemove the tags to remove
   */
  public void removeTags(Id.NamespacedId targetId, String ... tagsToRemove) {
    Set<String> existingTags = getTags(targetId);
    if (existingTags.isEmpty()) {
      // nothing to remove
      return;
    }

    Iterables.removeAll(existingTags, Arrays.asList(tagsToRemove));

    setTags(targetId, Iterables.toArray(existingTags, String.class));
  }

  /**
   * Removes all properties from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} for which to remove the properties
   */
  public void removeProperties(Id.NamespacedId targetId) {
    removeMetadata(targetId, MetadataType.PROPERTY);
  }

  /**
   * Removes all tags from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} for which to remove the tags
   */
  public void removeTags(Id.NamespacedId targetId) {
    removeMetadata(targetId, MetadataType.TAG);
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} based on key.
   *
   * @param value The metadata value to be found
   * @param type The target type of objects to search from
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the value
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnValue(String value, MetadataSearchTargetType type) {
    return executeSearchOnColumns(BusinessMetadataDataset.VALUE_COLUMN, value, type);
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} for key:value pair
   *
   * @param keyValue The metadata value to be found.
   * @param type The target type of objects to search from.
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the key value pair.
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnKeyValue(String keyValue, MetadataSearchTargetType type) {
    return executeSearchOnColumns(BusinessMetadataDataset.KEYVALUE_COLUMN, keyValue, type);
  }

  // Helper method to execute IndexedTable search on target index column.
  List<BusinessMetadataRecord> executeSearchOnColumns(String column, String searchValue,
                                                      MetadataSearchTargetType type) {
    List<BusinessMetadataRecord> results = new LinkedList<>();
    Scanner scanner = indexedTable.readByIndex(Bytes.toBytes(column), Bytes.toBytes(searchValue));
    try {
      Row next;
      while ((next = scanner.next()) != null) {
        String rowValue = next.getString(VALUE_COLUMN);
        if (rowValue == null) {
          continue;
        }

        final byte[] rowKey = next.getRow();
        String targetType = getTargetType(rowKey);

        // Filter on target type if not ALL
        if ((type != MetadataSearchTargetType.ALL) && (!targetType.equals(type.getInternalName()))) {
          continue;
        }

        Id.NamespacedId targetId = getNamespaceIdFromKey(targetType, new MDSKey(rowKey));
        String key = getMetadataKey(targetType, rowKey);
        String value = Bytes.toString(next.get(Bytes.toBytes(BusinessMetadataDataset.VALUE_COLUMN)));
        BusinessMetadataRecord record = new BusinessMetadataRecord(targetId, key, value);
        results.add(record);
      }
    } finally {
      scanner.close();
    }

    return results;
  }

  private void addNamespaceIdToKey(MDSKey.Builder builder, Id.NamespacedId namespacedId) {
    String type = getTargetType(namespacedId);
    if (type.equals(Id.Program.class.getSimpleName())) {
      Id.Program program = (Id.Program) namespacedId;
      String namespaceId = program.getNamespaceId();
      String appId = program.getApplicationId();
      String programType = program.getType().name();
      String programId = program.getId();
      builder.add(namespaceId);
      builder.add(appId);
      builder.add(programType);
      builder.add(programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      Id.Application application = (Id.Application) namespacedId;
      String namespaceId = application.getNamespaceId();
      String instanceId = application.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      Id.DatasetInstance datasetInstance = (Id.DatasetInstance) namespacedId;
      String namespaceId = datasetInstance.getNamespaceId();
      String instanceId = datasetInstance.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      Id.Stream stream = (Id.Stream) namespacedId;
      String namespaceId = stream.getNamespaceId();
      String instanceId = stream.getId();
      builder.add(namespaceId);
      builder.add(instanceId);
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
  }

  private Id.NamespacedId getNamespaceIdFromKey(String type, MDSKey key) {
    MDSKey.Splitter keySplitter = key.split();

    // The rowkey is [targetType][targetId][metadata-type][key], so skip the first string.
    keySplitter.skipString();
    if (type.equals(Id.Program.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      String programType = keySplitter.getString();
      String programId = keySplitter.getString();
      return Id.Program.from(namespaceId, appId, ProgramType.valueOf(programType), programId);
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String appId = keySplitter.getString();
      return Id.Application.from(namespaceId, appId);
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      String namespaceId = keySplitter.getString();
      String instanceId  = keySplitter.getString();
      return Id.DatasetInstance.from(namespaceId, instanceId);
    }
    throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
  }

  private void write(MDSKey id, BusinessMetadataRecord record) {
    Put put = new Put(id.getKey());

     // Now add the index columns.
    put.add(Bytes.toBytes(KEYVALUE_COLUMN), Bytes.toBytes(record.getKey() + KEYVALUE_SEPARATOR + record.getValue()));
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(record.getValue()));

    indexedTable.put(put);
  }

  // Helper method to generate key.
  private MDSKey getMDSKey(Id.NamespacedId targetId, MetadataType type, @Nullable String key) {
    String targetType = getTargetType(targetId);
    MDSKey.Builder builder = new MDSKey.Builder();
    builder.add(targetType);
    addNamespaceIdToKey(builder, targetId);
    builder.add(type.toString());
    if (key != null) {
      builder.add(key);
    }

    return builder.build();
  }

  private String getTargetType(Id.NamespacedId namespacedId) {
    if (namespacedId instanceof Id.Program) {
      return Id.Program.class.getSimpleName();
    }
    return namespacedId.getClass().getSimpleName();
  }

  private String getMetadataKey(String type, byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [targetType][targetId][metadata-type][key], so skip the first few strings.
    keySplitter.skipString();
    if (type.equals(Id.Program.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Application.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.DatasetInstance.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else if (type.equals(Id.Stream.class.getSimpleName())) {
      keySplitter.skipString();
      keySplitter.skipString();
      keySplitter.skipString();
    } else {
      throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
    }
    return keySplitter.getString();
  }

  private String getTargetType(byte[] rowKey) {
    MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
    // The rowkey is [targetType][targetId][metadata-type][key]
    return keySplitter.getString();
  }
}
