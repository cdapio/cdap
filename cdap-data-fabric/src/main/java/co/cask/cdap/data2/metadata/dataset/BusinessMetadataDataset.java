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
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  public static final String TAGS_KEY = "tags";
  public static final String TAGS_SEPARATOR = ",";

  // column keys
  static final String KEYVALUE_COLUMN = "kv";
  static final String VALUE_COLUMN = "v";
  static final String CASE_INSENSITIVE_VALUE_COLUMN = "civ";
  static final String HISTORY_COLUMN = "h";

  /**
   * Identifies the type of metadata - property or tag
   */
  enum MetadataType {
    PROPERTY("p"),
    TAG("t");

    private final String serializedForm;

    MetadataType(String serializedForm) {
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

    // Put to the default column.
    write(targetId, metadataType, metadataRecord);
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
    MDSKey mdsKey = MdsValueKey.getMDSKey(targetId, metadataType, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(VALUE_COLUMN);
    if (value == null) {
      // This can happen when all tags are moved one by one. The row still exists, but the value is null.
      return null;
    }

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
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsValueKey.getMDSKey(targetId, metadataType, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    Scanner scan = indexedTable.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String key = MdsValueKey.getMetadataKey(targetType, next.getRow());
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
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsValueKey.getMDSKey(targetId, metadataType, null);
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
        if (filter.apply(MdsValueKey.getMetadataKey(targetType, next.getRow()))) {
          indexedTable.delete(new Delete(next.getRow()));
        }
      }
    } finally {
      scan.close();
    }

    writeHistory(targetId);
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
   * @param namespaceId The namespace id to filter
   * @param value The metadata value to be found
   * @param type The target type of objects to search from
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the value
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnValue(String namespaceId, String value,
                                                                  MetadataSearchTargetType type) {
    return executeSearchOnColumns(namespaceId, BusinessMetadataDataset.CASE_INSENSITIVE_VALUE_COLUMN, value, type);
  }

  /**
   * Find the instance of {@link BusinessMetadataRecord} for key:value pair
   *
   * @param namespaceId The namespace id to filter
   * @param keyValue The metadata value to be found.
   * @param type The target type of objects to search from.
   * @return The {@link Iterable} of {@link BusinessMetadataRecord} that fit the key value pair.
   */
  public List<BusinessMetadataRecord> findBusinessMetadataOnKeyValue(String namespaceId, String keyValue,
                                                                     MetadataSearchTargetType type) {
    return executeSearchOnColumns(namespaceId, BusinessMetadataDataset.KEYVALUE_COLUMN, keyValue, type);
  }

  /**
   * Returns the snapshot of the metadata for entities on or before the given time.
   * @param targetIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  public Set<MetadataRecord> getSnapshotBeforeTime(Set<Id.NamespacedId> targetIds, long timeMillis) {
    ImmutableSet.Builder<MetadataRecord> builder = ImmutableSet.builder();
    for (Id.NamespacedId entityId : targetIds) {
      builder.add(getSnapshotBeforeTime(entityId, timeMillis));
    }
    return builder.build();
  }

  private MetadataRecord getSnapshotBeforeTime(Id.NamespacedId targetId, long timeMillis) {
    byte[] scanStartKey = MdsHistoryKey.getMdsScanStartKey(targetId, timeMillis).getKey();
    byte[] scanEndKey = MdsHistoryKey.getMdsScanEndKey(targetId).getKey();
    // TODO: add limit to scan, we need only one row
    Scanner scanner = indexedTable.scan(scanStartKey, scanEndKey);
    try {
      Row next = scanner.next();
      if (next != null) {
        return GSON.fromJson(next.getString(HISTORY_COLUMN), MetadataRecord.class);
      } else {
        return new MetadataRecord(targetId);
      }
    } finally {
      scanner.close();
    }
  }

  // Helper method to execute IndexedTable search on target index column.
  List<BusinessMetadataRecord> executeSearchOnColumns(final String namespaceId, final String column,
                                                      final String searchValue, final MetadataSearchTargetType type) {
    List<BusinessMetadataRecord> results = new LinkedList<>();

    Scanner scanner;
    String namespacedSearchValue = namespaceId + KEYVALUE_SEPARATOR + searchValue.toLowerCase();

    // Special case if the key is "tags:"
    // Since the value is saved as comma separated Strings, we need to do String matching to filter the right keyword.
    boolean modifiedTagsKVSearch = false;
    final String unmodifiedTagKeyValue = searchValue.toLowerCase();
    if (KEYVALUE_COLUMN.equals(column) && unmodifiedTagKeyValue.startsWith(TAGS_KEY + KEYVALUE_SEPARATOR)) {
      // If it is search on "tags" but does not end with * then we need to add virtual one.
      // Change the search to just search on namespace:tags: - we will do filter later.
      namespacedSearchValue =
        namespacedSearchValue.substring(0, namespacedSearchValue.lastIndexOf(KEYVALUE_SEPARATOR) + 1) + "*";
      modifiedTagsKVSearch = true;
    }

    if (namespacedSearchValue.endsWith("*")) {
      byte[] startKey = Bytes.toBytes(namespacedSearchValue.substring(0, namespacedSearchValue.lastIndexOf("*")));
      byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
      scanner = indexedTable.scanByIndex(Bytes.toBytes(column), startKey, stopKey);
    } else {
      byte[] value = Bytes.toBytes(namespacedSearchValue);
      scanner = indexedTable.readByIndex(Bytes.toBytes(column), value);
    }
    try {
      Row next;
      while ((next = scanner.next()) != null) {
        String rowValue = next.getString(VALUE_COLUMN);
        if (rowValue == null) {
          continue;
        }

        final byte[] rowKey = next.getRow();
        String targetType = MdsValueKey.getTargetType(rowKey);

        // Filter on target type if not ALL
        if ((type != MetadataSearchTargetType.ALL) && (!targetType.equals(type.getInternalName()))) {
          continue;
        }

        // Deal with special case for search on specific keyword for "tags:"
        if (modifiedTagsKVSearch) {
          boolean isMatch = false;
          if ((TAGS_KEY + KEYVALUE_SEPARATOR + "*").equals(unmodifiedTagKeyValue)) {
            isMatch = true;
          } else {
            Iterable<String> tagValues = Splitter.on(TAGS_SEPARATOR).omitEmptyStrings().trimResults().split(rowValue);
            for (String tagValue : tagValues) {
              // compare with tags:value
              String prefixedTagValue = TAGS_KEY + KEYVALUE_SEPARATOR + tagValue;
              if (!unmodifiedTagKeyValue.endsWith("*")) {
                if (prefixedTagValue.equals(unmodifiedTagKeyValue)) {
                  isMatch = true;
                  break;
                }
              } else {
                int endAsteriskIndex = unmodifiedTagKeyValue.lastIndexOf("*");
                if (prefixedTagValue.startsWith(unmodifiedTagKeyValue.substring(0, endAsteriskIndex))) {
                  isMatch = true;
                  break;
                }
              }
            }
          }
          if (!isMatch) {
            continue;
          }
        }

        Id.NamespacedId targetId = MdsValueKey.getNamespaceIdFromKey(targetType, new MDSKey(rowKey));
        String key = MdsValueKey.getMetadataKey(targetType, rowKey);
        String value = Bytes.toString(next.get(Bytes.toBytes(BusinessMetadataDataset.VALUE_COLUMN)));
        BusinessMetadataRecord record = new BusinessMetadataRecord(targetId, key, value);
        results.add(record);
      }
    } finally {
      scanner.close();
    }

    return results;
  }

  private void write(Id.NamespacedId targetId, MetadataType metadataType, BusinessMetadataRecord record) {
    String key = record.getKey();
    MDSKey mdsKey = MdsValueKey.getMDSKey(targetId, metadataType, key);
    Put put = new Put(mdsKey.getKey());

    // Now add the index columns. To support case insensitive we will store it as lower case for index columns.
    String lowerCaseKey = record.getKey().toLowerCase();
    String lowerCaseValue = record.getValue().toLowerCase();

    String nameSpacedKVIndexValue =
      MdsValueKey.getNamespaceId(mdsKey) + KEYVALUE_SEPARATOR + lowerCaseKey + KEYVALUE_SEPARATOR + lowerCaseValue;
    put.add(Bytes.toBytes(KEYVALUE_COLUMN), Bytes.toBytes(nameSpacedKVIndexValue));

    String nameSpacedVIndexValue = MdsValueKey.getNamespaceId(mdsKey) + KEYVALUE_SEPARATOR + lowerCaseValue;
    put.add(Bytes.toBytes(CASE_INSENSITIVE_VALUE_COLUMN), Bytes.toBytes(nameSpacedVIndexValue));

    // Add to value column
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(record.getValue()));

    indexedTable.put(put);

    writeHistory(targetId);
  }

  /**
   * Snapshots the metadata for the given targetId at the given time.
   * @param targetId target id for which metadata needs snapshotting
   */
  private void writeHistory(Id.NamespacedId targetId) {
    Map<String, String> properties = getProperties(targetId);
    Set<String> tags = getTags(targetId);
    MetadataRecord metadataRecord = new MetadataRecord(targetId, properties, tags);
    byte[] row = MdsHistoryKey.getMdsKey(targetId, System.currentTimeMillis()).getKey();
    indexedTable.put(row, Bytes.toBytes(HISTORY_COLUMN), Bytes.toBytes(GSON.toJson(metadataRecord)));
  }
}
