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
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Dataset that manages Metadata using an {@link IndexedTable}.
 */
public class MetadataDataset extends AbstractDataset {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  private static final Pattern VALUE_SPLIT_PATTERN = Pattern.compile("[-_\\s]+");
  private static final Pattern TAGS_SEPARATOR_PATTERN = Pattern.compile("[,\\s]+");

  private static final String HISTORY_COLUMN = "h"; // column for metadata history
  private static final String VALUE_COLUMN = "v";  // column for metadata value
  private static final String TAGS_SEPARATOR = ",";

  static final String INDEX_COLUMN = "i";          // column for metadata indexes

  public static final String TAGS_KEY = "tags";
  public static final String KEYVALUE_SEPARATOR = ":";

  private final IndexedTable indexedTable;

  public MetadataDataset(IndexedTable indexedTable) {
    super("metadataDataset", indexedTable);
    this.indexedTable = indexedTable;
  }

  /**
   * Add new metadata.
   *
   * @param metadataEntry The value of the metadata to be saved.
   */
  private void setMetadata(MetadataEntry metadataEntry) {
    Id.NamespacedId targetId = metadataEntry.getTargetId();

    // Put to the default column.
    write(targetId, metadataEntry);
  }

  /**
   * Sets a metadata property for the specified {@link Id.NamespacedId}.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public void setProperty(Id.NamespacedId targetId, String key, String value) {
    setMetadata(new MetadataEntry(targetId, key, value));
  }

  /**
   * Replaces existing tags of the specified {@link Id.NamespacedId} with a new set of tags.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param tags the tags to set
   */
  private void setTags(Id.NamespacedId targetId, String ... tags) {
    MetadataEntry tagsEntry = new MetadataEntry(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(tags));
    setMetadata(tagsEntry);
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
    MetadataEntry newTagsEntry = new MetadataEntry(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(newTags));
    setMetadata(newTagsEntry);
  }

  /**
   * Return metadata based on target id, and key.
   *
   * @param targetId The id of the target
   * @param key The metadata key to get
   * @return instance of {@link MetadataEntry} for the target type, id, and key
   */
  @Nullable
  private MetadataEntry getMetadata(Id.NamespacedId targetId, String key) {
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(VALUE_COLUMN);
    if (value == null) {
      // This can happen when all tags are moved one by one. The row still exists, but the value is null.
      return null;
    }

    return new MetadataEntry(targetId, key, Bytes.toString(value));
  }

  /**
   * Retrieve the {@link MetadataEntry} corresponding to the specified key for the {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which the {@link MetadataEntry} is to be retrieved
   * @param key the property key for which the {@link MetadataEntry} is to be retrieved
   * @return the {@link MetadataEntry} corresponding to the specified key for the {@link Id.NamespacedId}
   */
  @Nullable
  public MetadataEntry getProperty(Id.NamespacedId targetId, String key) {
    return getMetadata(targetId, key);
  }

  /**
   * Retrieves the metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the specified {@link Id.NamespacedId}
   * @return a Map representing the metadata for the specified {@link Id.NamespacedId}
   */
  private Map<String, String> getMetadata(Id.NamespacedId targetId) {
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    Scanner scan = indexedTable.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String key = MdsKey.getMetadataKey(targetType, next.getRow());
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
    Map<String, String> properties = getMetadata(targetId);
    properties.remove(TAGS_KEY); // remove tags
    return properties;
  }

  /**
   * Retrieves all the tags for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which tags are to be retrieved
   * @return the tags of the specified {@link Id.NamespacedId}
   */
  public Set<String> getTags(Id.NamespacedId targetId) {
    MetadataEntry tags = getMetadata(targetId, TAGS_KEY);
    if (tags == null) {
      return new HashSet<>();
    }
    return Sets.newHashSet(Splitter.on(TAGS_SEPARATOR).omitEmptyStrings().trimResults().split(tags.getValue()));
  }

  /**
   * Removes all metadata for the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which metadata is to be removed
   */
  private void removeMetadata(Id.NamespacedId targetId) {
    removeMetadata(targetId, Predicates.<String>alwaysTrue());
  }

  /**
   * Removes the specified keys from the metadata of the specified {@link Id.NamespacedId}.
   *
   * @param targetId the {@link Id.NamespacedId} for which the specified metadata keys are to be removed
   * @param keys the keys to remove from the metadata of the specified {@link Id.NamespacedId}
   */
  private void removeMetadata(Id.NamespacedId targetId, String ... keys) {
    final Set<String> keySet = Sets.newHashSet(keys);
    removeMetadata(targetId, new Predicate<String>() {
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
   * @param filter the {@link Predicate} that should be satisfied to remove a key
   */
  private void removeMetadata(Id.NamespacedId targetId, Predicate<String> filter) {
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] prefix = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    List<String> deletedMetadataKeys = new LinkedList<>();

    Scanner scan = indexedTable.scan(prefix, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String value = next.getString(VALUE_COLUMN);
        if (value == null) {
          continue;
        }
        String metadataKey = MdsKey.getMetadataKey(targetType, next.getRow());
        if (filter.apply(metadataKey)) {
          indexedTable.delete(new Delete(next.getRow()));
          // store the key to delete its indexes later
          deletedMetadataKeys.add(metadataKey);
        }
      }
    } finally {
      scan.close();
    }

    // delete all the indexes for all deleted metadata key
    for (String deletedMetadataKey : deletedMetadataKeys) {
      deleteIndexes(targetId, deletedMetadataKey);
    }

    writeHistory(targetId);
  }

  /**
   * Deletes all indexes associated with a metadata key
   *
   * @param targetId the {@link Id.NamespacedId} for which keys are to be removed
   * @param metadataKey the key to remove from the metadata of the specified {@link Id.NamespacedId}
   */
  private void deleteIndexes(Id.NamespacedId targetId, String metadataKey) {
    MDSKey mdsKey = MdsKey.getMDSIndexKey(targetId, metadataKey, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Scanner scan = indexedTable.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        String index = next.getString(INDEX_COLUMN);
        if (index == null) {
          continue;
        }
        indexedTable.delete(new Delete(next.getRow()));
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
    removeMetadata(targetId, keys);
  }

  /**
   * Removes the specified tags from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} from which to remove the specified tags
   * @param tagsToRemove the tags to remove
   */
  public void removeTags(Id.NamespacedId targetId, String... tagsToRemove) {
    Set<String> existingTags = getTags(targetId);
    if (existingTags.isEmpty()) {
      // nothing to remove
      return;
    }

    Iterables.removeAll(existingTags, Arrays.asList(tagsToRemove));

    // call remove metadata for tags which will delete all the existing indexes for tags of this targetId
    removeMetadata(targetId, TAGS_KEY);
    setTags(targetId, Iterables.toArray(existingTags, String.class));
  }

  /**
   * Removes all properties from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} for which to remove the properties
   */
  public void removeProperties(Id.NamespacedId targetId) {
    removeMetadata(targetId);
  }

  /**
   * Removes all tags from the specified entity.
   *
   * @param targetId the {@link Id.NamespacedId} for which to remove the tags
   */
  public void removeTags(Id.NamespacedId targetId) {
    removeMetadata(targetId);
  }

  /**
   * Returns the snapshot of the metadata for entities on or before the given time.
   * @param targetIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  public Set<Metadata> getSnapshotBeforeTime(Set<Id.NamespacedId> targetIds, long timeMillis) {
    ImmutableSet.Builder<Metadata> builder = ImmutableSet.builder();
    for (Id.NamespacedId entityId : targetIds) {
      builder.add(getSnapshotBeforeTime(entityId, timeMillis));
    }
    return builder.build();
  }

  private Metadata getSnapshotBeforeTime(Id.NamespacedId targetId, long timeMillis) {
    byte[] scanStartKey = MdsHistoryKey.getMdsScanStartKey(targetId, timeMillis).getKey();
    byte[] scanEndKey = MdsHistoryKey.getMdsScanEndKey(targetId).getKey();
    // TODO: add limit to scan, we need only one row
    Scanner scanner = indexedTable.scan(scanStartKey, scanEndKey);
    try {
      Row next = scanner.next();
      if (next != null) {
        return GSON.fromJson(next.getString(HISTORY_COLUMN), Metadata.class);
      } else {
        return new Metadata(targetId);
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * Performs a search for the given search query in the given namespace for the given {@link MetadataSearchTargetType}
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value] and can have '*'
   * at the end for a prefixed search.
   * @param type the {@link MetadataSearchTargetType} to restrict the search to
   */
  public List<MetadataEntry> search(final String namespaceId, final String searchQuery,
                                    final MetadataSearchTargetType type) {
    Set<MetadataEntry> results = new HashSet<>();
    Scanner scanner;

    String namespacedSearchQuery = prepareSearchQuery(namespaceId, searchQuery);

    if (namespacedSearchQuery.endsWith("*")) {
      // if prefixed search get start and stop key
      byte[] startKey = Bytes.toBytes(namespacedSearchQuery.substring(0, namespacedSearchQuery.lastIndexOf("*")));
      byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
      scanner = indexedTable.scanByIndex(Bytes.toBytes(INDEX_COLUMN), startKey, stopKey);
    } else {
      byte[] value = Bytes.toBytes(namespacedSearchQuery);
      scanner = indexedTable.readByIndex(Bytes.toBytes(INDEX_COLUMN), value);
    }
    try {
      Row next;
      while ((next = scanner.next()) != null) {
        String rowValue = next.getString(INDEX_COLUMN);
        if (rowValue == null) {
          continue;
        }

        final byte[] rowKey = next.getRow();
        String targetType = MdsKey.getTargetType(rowKey);

        // Filter on target type if not ALL
        if ((type != MetadataSearchTargetType.ALL) && (type !=
          MetadataSearchTargetType.valueOfSerializedForm(targetType))) {
          continue;
        }

        Id.NamespacedId targetId = MdsKey.getNamespaceIdFromKey(targetType, rowKey);
        String key = MdsKey.getMetadataKey(targetType, rowKey);
        MetadataEntry entry = getMetadata(targetId, key);
        results.add(entry);
      }
    } finally {
      scanner.close();
    }
    return Lists.newArrayList(results);
  }

  /**
   * Prepares a search query by trimming and also removing whitespaces around the {@link #KEYVALUE_SEPARATOR} and
   * appending the given namespaceId to perform search in a namespace.
   *
   * @param namespaceId the namespaceId to search in
   * @param searchQuery the user specified search query
   * @return formatted search query which is namespaced
   */
  private String prepareSearchQuery(String namespaceId, String searchQuery) {
    String formattedSearchQuery = searchQuery.toLowerCase().trim();
    // if this is a key:value search remove  spaces around the separator too
    if (formattedSearchQuery.contains(KEYVALUE_SEPARATOR)) {
      // split the search query in two parts on first occurrence of KEYVALUE_SEPARATOR and the trim the key and value
      String[] split = formattedSearchQuery.split(KEYVALUE_SEPARATOR, 2);
      formattedSearchQuery = split[0].trim() + KEYVALUE_SEPARATOR + split[1].trim();
    }
    return namespaceId + KEYVALUE_SEPARATOR + formattedSearchQuery;
  }

  private void write(Id.NamespacedId targetId, MetadataEntry entry) {
    String key = entry.getKey();
    MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
    indexedTable.put(put);
    // index the metadata
    storeIndexes(targetId, entry);

    writeHistory(targetId);
  }

  /**
   * Store indexes for a {@link MetadataEntry}
   *
   * @param targetId the {@link Id.NamespacedId} from which the metadata indexes has to be stored
   * @param entry the {@link MetadataEntry} which has to be indexed
   */
  private void storeIndexes(Id.NamespacedId targetId, MetadataEntry entry) {
    Set<String> valueIndexes = new HashSet<>();
    if (entry.getValue().contains(TAGS_SEPARATOR)) {
      // if the entry is tag then each tag is an index
      valueIndexes.addAll(Arrays.asList(TAGS_SEPARATOR_PATTERN.split(entry.getValue())));
    } else {
      // for key value the complete value is an index
      valueIndexes.add(entry.getValue());
    }
    Set<String> indexes = Sets.newHashSet();
    for (String index : valueIndexes) {
      // split all value indexes on the VALUE_SPLIT_PATTERN
      indexes.addAll(Arrays.asList(VALUE_SPLIT_PATTERN.split(index)));
    }
    // add all value indexes too
    indexes.addAll(valueIndexes);

    for (String index : indexes) {
      // store the index with key of the metadata
      indexedTable.put(getIndexPut(targetId, entry.getKey(), entry.getKey() + KEYVALUE_SEPARATOR + index));
      // store just the index value
      indexedTable.put(getIndexPut(targetId, entry.getKey(), index));
    }
  }

  /**
   * Creates a {@link Put} for the a metadata index
   *
   * @param targetId the {@link Id.NamespacedId} from which the metadata index has to be created
   * @param metadataKey the key of the metadata entry
   * @param index the index for this metadata
   * @return {@link Put} which is a index row with the value to be indexed in the {@link #INDEX_COLUMN}
   */
  private Put getIndexPut(Id.NamespacedId targetId, String metadataKey, String index) {
    MDSKey mdsIndexKey = MdsKey.getMDSIndexKey(targetId, metadataKey, index.toLowerCase());
    String namespacedIndex = MdsKey.getNamespaceId(mdsIndexKey) + KEYVALUE_SEPARATOR + index.toLowerCase();
    Put put = new Put(mdsIndexKey.getKey());
    put.add(Bytes.toBytes(INDEX_COLUMN), Bytes.toBytes(namespacedIndex));
    return put;
  }

  /**
   * Snapshots the metadata for the given targetId at the given time.
   * @param targetId target id for which metadata needs snapshotting
   */
  private void writeHistory(Id.NamespacedId targetId) {
    Map<String, String> properties = getProperties(targetId);
    Set<String> tags = getTags(targetId);
    Metadata metadata = new Metadata(targetId, properties, tags);
    byte[] row = MdsHistoryKey.getMdsKey(targetId, System.currentTimeMillis()).getKey();
    indexedTable.put(row, Bytes.toBytes(HISTORY_COLUMN), Bytes.toBytes(GSON.toJson(metadata)));
  }
}
