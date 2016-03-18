/*
 * Copyright 2015-2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.metadata.indexer.DefaultValueIndexer;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
  private static final Logger LOG = LoggerFactory.getLogger(MetadataDataset.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");

  private static final String HISTORY_COLUMN = "h"; // column for metadata history
  private static final String VALUE_COLUMN = "v";  // column for metadata value
  private static final String TAGS_SEPARATOR = ",";

  // Fuzzy key is of form <row key, key mask>. We want to compare row keys.
  private static final Comparator<ImmutablePair<byte[], byte[]>> FUZZY_KEY_COMPARATOR =
    new Comparator<ImmutablePair<byte[], byte[]>>() {
      @Override
      public int compare(ImmutablePair<byte[], byte[]> o1, ImmutablePair<byte[], byte[]> o2) {
        return Bytes.compareTo(o1.getFirst(), o2.getFirst());
      }
    };

  //TODO: (UPG-3.3): Make this public after 3.3
  public static final String INDEX_COLUMN = "i";          // column for metadata indexes

  //TODO: (UPG-3.3): Remove this after 3.3. This is only for upgrade from 3.2 to 3.3
  // These are the columns which were indexed in 3.2
  public static final String CASE_INSENSITIVE_VALUE_COLUMN = "civ";
  public static final String KEYVALUE_COLUMN = "kv";

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
   * @param indexer the indexer to use to create indexes for this {@link MetadataEntry}
   */
  private void setMetadata(MetadataEntry metadataEntry, @Nullable Indexer indexer) {
    Id.NamespacedId targetId = metadataEntry.getTargetId();

    // Put to the default column.
    write(targetId, metadataEntry, indexer == null ? new DefaultValueIndexer() : indexer);
  }

  /**
   * Sets a metadata property for the specified {@link Id.NamespacedId}.
   *
   * @param targetId The target Id: {@link Id.Application} / {@link Id.Program} /
   *                 {@link Id.DatasetInstance}/{@link Id.Stream}
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public void setProperty(Id.NamespacedId targetId, String key, String value) {
    setProperty(targetId, key, value, null);
  }

  /**
   * Sets a metadata property for the specified {@link Id.NamespacedId}.
   *
   * @param targetId The target Id: {@link Id.Application} / {@link Id.Program} /
   *                 {@link Id.DatasetInstance}/{@link Id.Stream}
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   * @param indexer the indexer to use to create indexes for this key-value property
   */
  public void setProperty(Id.NamespacedId targetId, String key, String value, @Nullable Indexer indexer) {
    setMetadata(new MetadataEntry(targetId, key, value), indexer);
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
    setMetadata(tagsEntry, null);
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
    setMetadata(newTagsEntry, null);
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
    return splitTags(tags.getValue());
  }

  private static HashSet<String> splitTags(String tags) {
    return Sets.newHashSet(Splitter.on(TAGS_SEPARATOR).omitEmptyStrings().trimResults().split(tags));
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
   * Returns metadata for a given set of entities
   *
   * @param targetIds entities for which metadata is required
   * @return map of entitiyId to set of metadata for that entity
   */
  public Set<Metadata> getMetadata(Set<? extends Id.NamespacedId> targetIds) {
    List<ImmutablePair<byte [], byte []>> fuzzyKeys = new ArrayList<>();
    for (Id.NamespacedId targetId : targetIds) {
      fuzzyKeys.add(getFuzzyKeyFor(targetId));
    }

    // Sort fuzzy keys
    Collections.sort(fuzzyKeys, FUZZY_KEY_COMPARATOR);

    // Scan using fuzzy filter. Scan returns one row per property.
    // Group the rows on entityId
    Multimap<Id.NamespacedId, MetadataEntry> metadataMap = HashMultimap.create();
    Scanner scan = indexedTable.scan(new Scan(null, null, new FuzzyRowFilter(fuzzyKeys)));
    try {
      Row next;
      while ((next = scan.next()) != null) {
        MetadataEntry metadataEntry = convertRow(next);
        if (metadataEntry != null) {
          metadataMap.put(metadataEntry.getTargetId(), metadataEntry);
        }
      }
    } finally {
      scan.close();
    }

    // Create metadata objects for each entity from grouped rows
    Set<Metadata> metadataSet = new HashSet<>();
    for (Map.Entry<Id.NamespacedId, Collection<MetadataEntry>> entry : metadataMap.asMap().entrySet()) {
      Map<String, String> properties = new HashMap<>();
      Set<String> tags = Collections.emptySet();
      for (MetadataEntry metadataEntry : entry.getValue()) {
        if (TAGS_KEY.equals(metadataEntry.getKey())) {
          tags = splitTags(metadataEntry.getValue());
        } else {
          properties.put(metadataEntry.getKey(), metadataEntry.getValue());
        }
      }
      metadataSet.add(new Metadata(entry.getKey(), properties, tags));
    }
    return metadataSet;
  }

  @Nullable
  private MetadataEntry convertRow(Row row) {
    byte[] rowKey = row.getRow();
    String targetType = MdsKey.getTargetType(rowKey);
    Id.NamespacedId namespacedId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
    String key = MdsKey.getMetadataKey(targetType, rowKey);
    byte[] value = row.get(VALUE_COLUMN);
    if (key == null || value == null) {
      return null;
    }
    return new MetadataEntry(namespacedId, key, Bytes.toString(value));
  }

  private ImmutablePair<byte[], byte[]> getFuzzyKeyFor(Id.NamespacedId targetId) {
    // We need to create fuzzy pairs to match the first part of the key containing targetId
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] keyBytes = mdsKey.getKey();
    // byte array is automatically initialized to 0, which implies fixed match in fuzzy info
    // the row key after targetId doesn't need to be a match.
    byte[] infoBytes = new byte[keyBytes.length];

    return new ImmutablePair<>(keyBytes, infoBytes);
  }

  /**
   * Searches entities that match the specified search query in the specified namespace and {@link Id.Namespace#SYSTEM}
   * for the specified {@link MetadataSearchTargetType}.
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value] and can have '*'
   *                    at the end for a prefix search
   * @param type the {@link MetadataSearchTargetType} to restrict the search to.
   */
  public List<MetadataEntry> search(String namespaceId, String searchQuery, MetadataSearchTargetType type) {
    List<MetadataEntry> results = new ArrayList<>();
    for (String searchTerm : getSearchTerms(namespaceId, searchQuery)) {
      Scanner scanner;
      if (searchTerm.endsWith("*")) {
        // if prefixed search get start and stop key
        byte[] startKey = Bytes.toBytes(searchTerm.substring(0, searchTerm.lastIndexOf("*")));
        byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
        scanner = indexedTable.scanByIndex(Bytes.toBytes(INDEX_COLUMN), startKey, stopKey);
      } else {
        byte[] value = Bytes.toBytes(searchTerm);
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
          if ((type != MetadataSearchTargetType.ALL) &&
            (type != MetadataSearchTargetType.valueOfSerializedForm(targetType))) {
            continue;
          }

          Id.NamespacedId targetId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
          String key = MdsKey.getMetadataKey(targetType, rowKey);
          MetadataEntry entry = getMetadata(targetId, key);
          results.add(entry);
        }
      } finally {
        scanner.close();
      }
    }
    return results;
  }

  /**
   * Prepares search terms from the specified search query by
   * <ol>
   *   <li>Splitting on {@link #SPACE_SEPARATOR_PATTERN} and trimming</li>
   *   <li>Handling {@link #KEYVALUE_SEPARATOR}, so searches of the pattern key:value* can be supported</li>
   *   <li>Prepending the result with the specified namespaceId and {@link Id.Namespace#SYSTEM} so the search can
   *   be restricted to entities in the specified namespace and {@link Id.Namespace#SYSTEM}.</li>
   * </ol>t
   *
   * @param namespaceId the namespaceId to search in
   * @param searchQuery the user specified search query
   * @return formatted search query which is namespaced
   */
  private Iterable<String> getSearchTerms(String namespaceId, String searchQuery) {
    List<String> searchTerms = new ArrayList<>();
    for (String term : Splitter.on(SPACE_SEPARATOR_PATTERN).omitEmptyStrings().trimResults().split(searchQuery)) {
      String formattedSearchTerm = term.toLowerCase();
      // if this is a key:value search remove  spaces around the separator too
      if (formattedSearchTerm.contains(KEYVALUE_SEPARATOR)) {
        // split the search query in two parts on first occurrence of KEYVALUE_SEPARATOR and the trim the key and value
        String[] split = formattedSearchTerm.split(KEYVALUE_SEPARATOR, 2);
        formattedSearchTerm = split[0].trim() + KEYVALUE_SEPARATOR + split[1].trim();
      }
      searchTerms.add(namespaceId + KEYVALUE_SEPARATOR + formattedSearchTerm);
      // for non-system namespaces, also add the system namespace, so entities from system namespace are surfaced
      // in the search results as well
      if (!Id.Namespace.SYSTEM.getId().equals(namespaceId)) {
        searchTerms.add(Id.Namespace.SYSTEM.getId() + KEYVALUE_SEPARATOR + formattedSearchTerm);
      }
    }
    return searchTerms;
  }

  private void write(Id.NamespacedId targetId, MetadataEntry entry, Indexer indexer) {
    String key = entry.getKey();
    MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
    indexedTable.put(put);
    storeIndexes(targetId, entry, indexer.getIndexes(entry));
    writeHistory(targetId);
  }

  /**
   * Store indexes for a {@link MetadataEntry}
   *
   * @param targetId the {@link Id.NamespacedId} from which the metadata indexes has to be stored
   * @param entry the {@link MetadataEntry} which has to be indexed
   * @param indexes {@link Set<String>} of indexes to store for this {@link MetadataEntry}
   */
  private void storeIndexes(Id.NamespacedId targetId, MetadataEntry entry, Set<String> indexes) {
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

  /**
   * Upgrades the metadata from 3.2 to 3.3 for new storage format
   */
  public void upgrade() {
    new Upgrader().upgrade();
  }

  /**
   * Upgrader class for {@link MetadataDataset}. This class contains some functions from 3.2 which have changed in 3.3
   * in their only 3.2 format to help in reading and processing the old metadata. This inner class should be deleted
   * after 3.3
   */
  private class Upgrader {
    public void upgrade() {
      boolean upgradePerformed = false;
      Scanner scan = indexedTable.scan(null, null); // scan the whole table
      try {
        Row next;
        while ((next = scan.next()) != null) {
          byte[] value = next.get(VALUE_COLUMN);
          if (next.get(CASE_INSENSITIVE_VALUE_COLUMN) != null && value != null) {
            // if case insensitive column has value then this is an old entry from 3.2 which needs to be updated.
            final byte[] rowKey = next.getRow();
            String targetType = MdsKey.getTargetType(rowKey);
            Id.NamespacedId targetId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
            String key = getMetadataKeyFromOldFormat(targetType, rowKey);
            MetadataEntry metadataEntry = new MetadataEntry(targetId, key, Bytes.toString(value));
            indexedTable.delete(new Delete(rowKey));            // remove the old metadata entry
            // add the metadata back creating new indexes. We can just use DefaultValueIndexer because
            // in 3.2 we didn't have schema as metadata so no old records can be schema.
            write(metadataEntry.getTargetId(), metadataEntry, new DefaultValueIndexer());
            upgradePerformed = true;
            LOG.info("Upgraded MetadataEntry: {}", metadataEntry);
          }
        }
      } finally {
        scan.close();
      }
      if (!upgradePerformed) {
        LOG.info("No MetadataEntry found in old format. Metadata upgrade not required.");
      }
    }

    /**
     * Write similar to {@link MetadataDataset#write(Id.NamespacedId, MetadataEntry, Indexer)} but without history
     * since while writting during upgrade we don't want to add history.
     */
    private void write(Id.NamespacedId targetId, MetadataEntry entry, Indexer indexer) {
      String key = entry.getKey();
      MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
      Put put = new Put(mdsValueKey.getKey());

      // add the metadata value
      put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
      indexedTable.put(put);
      // index the metadata
      storeIndexes(targetId, entry, indexer.getIndexes(entry));
    }

    /**
     * This to read old Metadata keys which were written with a metadata type in 3.2
     * Its used to update from 3.2 to 3.3 and should be removed after 3.3
     * This is almost a copy of {@link MdsKey#getMetadataKey(String, byte[])} with additional skip for MetadataType
     * which we used to write in 3.2
     */
    public String getMetadataKeyFromOldFormat(String type, byte[] rowKey) {
      MDSKey.Splitter keySplitter = new MDSKey(rowKey).split();
      // The rowkey in the following format in 3.2
      // [rowPrefix][targetType][targetId][metadataType][key]
      // so skip the first few strings.

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
      } else {
        throw new IllegalArgumentException("Illegal Type " + type + " of metadata source.");
      }

      // Skip metadata-type as the old key as metadata type
      keySplitter.getString();

      return keySplitter.getString();
    }
  }
}
