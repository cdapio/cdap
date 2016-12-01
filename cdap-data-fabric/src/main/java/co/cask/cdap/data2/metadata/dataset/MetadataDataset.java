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
import co.cask.cdap.data2.metadata.indexer.SchemaIndexer;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
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
    .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
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
   * @param indexer the indexer to use to create indexes for this {@link MetadataEntry}
   */
  private void setMetadata(MetadataEntry metadataEntry, @Nullable Indexer indexer) {
    NamespacedEntityId targetId = metadataEntry.getTargetId();

    // Put to the default column.
    write(targetId, metadataEntry, indexer == null ? new DefaultValueIndexer() : indexer);
  }

  /**
   * Sets a metadata property for the specified {@link NamespacedEntityId}.
   *
   * @param targetId The target Id: {@link ApplicationId} / {@link ProgramId} / {@link DatasetId}/ {@link StreamId}
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public void setProperty(NamespacedEntityId targetId, String key, String value) {
    setProperty(targetId, key, value, null);
  }

  /**
   * Sets a metadata property for the specified {@link NamespacedEntityId}.
   *
   * @param targetId The target Id: {@link ApplicationId} / {@link ProgramId} /
   *                 {@link DatasetId}/{@link StreamId}
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   * @param indexer the indexer to use to create indexes for this key-value property
   */
  public void setProperty(NamespacedEntityId targetId, String key, String value, @Nullable Indexer indexer) {
    setMetadata(new MetadataEntry(targetId, key, value), indexer);
  }

  /**
   * Replaces existing tags of the specified {@link NamespacedEntityId} with a new set of tags.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param tags the tags to set
   */
  private void setTags(NamespacedEntityId targetId, String ... tags) {
    MetadataEntry tagsEntry = new MetadataEntry(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(tags));
    setMetadata(tagsEntry, null);
  }

  /**
   * Adds a new tag for the specified {@link NamespacedEntityId}.
   *
   * @param targetId the target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream).
   * @param tagsToAdd the tags to add
   */
  public void addTags(NamespacedEntityId targetId, String ... tagsToAdd) {
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
  private MetadataEntry getMetadata(NamespacedEntityId targetId, String key) {
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
   * Retrieve the {@link MetadataEntry} corresponding to the specified key for the {@link NamespacedEntityId}.
   *
   * @param targetId the {@link NamespacedEntityId} for which the {@link MetadataEntry} is to be retrieved
   * @param key the property key for which the {@link MetadataEntry} is to be retrieved
   * @return the {@link MetadataEntry} corresponding to the specified key for the {@link NamespacedEntityId}
   */
  @Nullable
  public MetadataEntry getProperty(NamespacedEntityId targetId, String key) {
    return getMetadata(targetId, key);
  }

  /**
   * Retrieves the metadata for the specified {@link NamespacedEntityId}.
   *
   * @param targetId the specified {@link NamespacedEntityId}
   * @return a Map representing the metadata for the specified {@link NamespacedEntityId}
   */
  private Map<String, String> getMetadata(NamespacedEntityId targetId) {
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    try (Scanner scan = indexedTable.scan(startKey, stopKey)) {
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
    }
  }

  /**
   * Retrieves all the properties for the specified {@link NamespacedEntityId}.
   *
   * @param targetId the {@link NamespacedEntityId} for which properties are to be retrieved
   * @return the properties of the specified {@link NamespacedEntityId}
   */
  public Map<String, String> getProperties(NamespacedEntityId targetId) {
    Map<String, String> properties = getMetadata(targetId);
    properties.remove(TAGS_KEY); // remove tags
    return properties;
  }

  /**
   * Retrieves all the tags for the specified {@link NamespacedEntityId}.
   *
   * @param targetId the {@link NamespacedEntityId} for which tags are to be retrieved
   * @return the tags of the specified {@link NamespacedEntityId}
   */
  public Set<String> getTags(NamespacedEntityId targetId) {
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
   * Removes the specified keys from the metadata of the specified {@link NamespacedEntityId}.
   *
   * @param targetId the {@link NamespacedEntityId} for which the specified metadata keys are to be removed
   * @param keys the keys to remove from the metadata of the specified {@link NamespacedEntityId}
   */
  private void removeMetadata(NamespacedEntityId targetId, String ... keys) {
    final Set<String> keySet = Sets.newHashSet(keys);
    removeMetadata(targetId, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return keySet.contains(input);
      }
    });
  }

  /**
   * Removes all keys that satisfy a given predicate from the metadata of the specified {@link NamespacedEntityId}.
   *
   * @param targetId the {@link NamespacedEntityId} for which keys are to be removed
   * @param filter the {@link Predicate} that should be satisfied to remove a key
   */
  private void removeMetadata(NamespacedEntityId targetId, Predicate<String> filter) {
    String targetType = KeyHelper.getTargetType(targetId);
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] prefix = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    List<String> deletedMetadataKeys = new LinkedList<>();

    try (Scanner scan = indexedTable.scan(prefix, stopKey)) {
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
   * @param targetId the {@link NamespacedEntityId} for which keys are to be removed
   * @param metadataKey the key to remove from the metadata of the specified {@link NamespacedEntityId}
   */
  private void deleteIndexes(NamespacedEntityId targetId, String metadataKey) {
    MDSKey mdsKey = MdsKey.getMDSIndexKey(targetId, metadataKey, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    try (Scanner scan = indexedTable.scan(startKey, stopKey)) {
      Row next;
      while ((next = scan.next()) != null) {
        deleteIndexRow(next);
      }
    }
  }

  /**
   * Removes the specified keys from the metadata properties of an entity.
   *
   * @param targetId the {@link NamespacedEntityId} from which to remove the specified keys
   * @param keys the keys to remove
   */
  public void removeProperties(NamespacedEntityId targetId, String ... keys) {
    removeMetadata(targetId, keys);
  }

  /**
   * Removes the specified tags from the specified entity.
   *
   * @param targetId the {@link NamespacedEntityId} from which to remove the specified tags
   * @param tagsToRemove the tags to remove
   */
  public void removeTags(NamespacedEntityId targetId, String... tagsToRemove) {
    Set<String> existingTags = getTags(targetId);
    if (existingTags.isEmpty()) {
      // nothing to remove
      return;
    }

    Iterables.removeAll(existingTags, Arrays.asList(tagsToRemove));

    // call remove metadata for tags which will delete all the existing indexes for tags of this targetId
    removeMetadata(targetId, TAGS_KEY);
    //check if tags are all deleted before set Tags, if tags are all deleted, a null value will be set to the targetId,
    //which will give a NPE later when searchMetadataOnType.
    if (!existingTags.isEmpty()) {
      setTags(targetId, Iterables.toArray(existingTags, String.class));
    }
  }

  /**
   * Removes all properties from the specified entity.
   *
   * @param targetId the {@link NamespacedEntityId} for which to remove the properties
   */
  public void removeProperties(NamespacedEntityId targetId) {
    removeMetadata(targetId,
                   new Predicate<String>() {
                     @Override
                     public boolean apply(String input) {
                       return !TAGS_KEY.equals(input);
                     }
                   });
  }

  /**
   * Removes all tags from the specified entity.
   *
   * @param targetId the {@link NamespacedEntityId} for which to remove the tags
   */
  public void removeTags(NamespacedEntityId targetId) {
    removeMetadata(targetId,
                   new Predicate<String>() {
                     @Override
                     public boolean apply(String input) {
                       return TAGS_KEY.equals(input);
                     }
                   });
  }

  /**
   * Returns the snapshot of the metadata for entities on or before the given time.
   * @param targetIds entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  public Set<Metadata> getSnapshotBeforeTime(Set<NamespacedEntityId> targetIds, long timeMillis) {
    ImmutableSet.Builder<Metadata> builder = ImmutableSet.builder();
    for (NamespacedEntityId namespacedEntityId : targetIds) {
      builder.add(getSnapshotBeforeTime(namespacedEntityId, timeMillis));
    }
    return builder.build();
  }

  private Metadata getSnapshotBeforeTime(NamespacedEntityId targetId, long timeMillis) {
    byte[] scanStartKey = MdsHistoryKey.getMdsScanStartKey(targetId, timeMillis).getKey();
    byte[] scanEndKey = MdsHistoryKey.getMdsScanEndKey(targetId).getKey();
    // TODO: add limit to scan, we need only one row
    try (Scanner scanner = indexedTable.scan(scanStartKey, scanEndKey)) {
      Row next = scanner.next();
      if (next != null) {
        return GSON.fromJson(next.getString(HISTORY_COLUMN), Metadata.class);
      } else {
        return new Metadata(targetId);
      }
    }
  }

  /**
   * Returns metadata for a given set of entities
   *
   * @param targetIds entities for which metadata is required
   * @return map of entitiyId to set of metadata for that entity
   */
  public Set<Metadata> getMetadata(Set<? extends NamespacedEntityId> targetIds) {
    if (targetIds.isEmpty()) {
      return Collections.emptySet();
    }

    List<ImmutablePair<byte [], byte []>> fuzzyKeys = new ArrayList<>();
    for (NamespacedEntityId targetId : targetIds) {
      fuzzyKeys.add(getFuzzyKeyFor(targetId));
    }

    // Sort fuzzy keys
    Collections.sort(fuzzyKeys, FUZZY_KEY_COMPARATOR);

    // Scan using fuzzy filter. Scan returns one row per property.
    // Group the rows on namespacedId
    Multimap<NamespacedEntityId, MetadataEntry> metadataMap = HashMultimap.create();
    byte[] start = fuzzyKeys.get(0).getFirst();
    byte[] end = Bytes.stopKeyForPrefix(fuzzyKeys.get(fuzzyKeys.size() - 1).getFirst());
    try (Scanner scan = indexedTable.scan(new Scan(start, end, new FuzzyRowFilter(fuzzyKeys)))) {
      Row next;
      while ((next = scan.next()) != null) {
        MetadataEntry metadataEntry = convertRow(next);
        if (metadataEntry != null) {
          metadataMap.put(metadataEntry.getTargetId(), metadataEntry);
        }
      }
    }

    // Create metadata objects for each entity from grouped rows
    Set<Metadata> metadataSet = new HashSet<>();
    for (Map.Entry<NamespacedEntityId, Collection<MetadataEntry>> entry : metadataMap.asMap().entrySet()) {
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
    NamespacedEntityId namespacedEntityId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
    String key = MdsKey.getMetadataKey(targetType, rowKey);
    byte[] value = row.get(VALUE_COLUMN);
    if (key == null || value == null) {
      return null;
    }
    return new MetadataEntry(namespacedEntityId, key, Bytes.toString(value));
  }

  private ImmutablePair<byte[], byte[]> getFuzzyKeyFor(NamespacedEntityId targetId) {
    // We need to create fuzzy pairs to match the first part of the key containing targetId
    MDSKey mdsKey = MdsKey.getMDSValueKey(targetId, null);
    byte[] keyBytes = mdsKey.getKey();
    // byte array is automatically initialized to 0, which implies fixed match in fuzzy info
    // the row key after targetId doesn't need to be a match.
    // Workaround for HBASE-15676, need to have at least one 1 in the fuzzy filter
    byte[] infoBytes = new byte[keyBytes.length + 1];
    infoBytes[infoBytes.length - 1] = 1;

    // the key array size and mask array size has to be equal so increase the size by 1
    return new ImmutablePair<>(Bytes.concat(keyBytes, new byte[1]), infoBytes);
  }

  /**
   * Searches entities that match the specified search query in the specified namespace and {@link NamespaceId#SYSTEM}
   * for the specified {@link MetadataSearchTargetType}.
   *
   * @param namespaceId the namespace to search in
   * @param searchQuery the search query, which could be of two forms: [key]:[value] or just [value] and can have '*'
   *                    at the end for a prefix search
   * @param types the {@link MetadataSearchTargetType} to restrict the search to, if empty all types are searched
   */
  public List<MetadataEntry> search(String namespaceId, String searchQuery, Set<MetadataSearchTargetType> types) {
    boolean includeAllTypes = types.isEmpty() || types.contains(MetadataSearchTargetType.ALL);
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

          // Filter on target type if not set to include all types
          if (!includeAllTypes &&
            !types.contains(MetadataSearchTargetType.valueOfSerializedForm(targetType))) {
            continue;
          }

          NamespacedEntityId targetId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
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
   *   <li>Prepending the result with the specified namespaceId and {@link NamespaceId#SYSTEM} so the search can
   *   be restricted to entities in the specified namespace and {@link NamespaceId#SYSTEM}.</li>
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
      if (!NamespaceId.SYSTEM.getEntityName().equals(namespaceId)) {
        searchTerms.add(NamespaceId.SYSTEM.getEntityName() + KEYVALUE_SEPARATOR + formattedSearchTerm);
      }
    }
    return searchTerms;
  }

  private void write(NamespacedEntityId targetId, MetadataEntry entry, Indexer indexer) {
    String key = entry.getKey();
    MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
    indexedTable.put(put);
    storeIndexes(targetId, entry.getKey(), indexer.getIndexes(entry));
    writeHistory(targetId);
  }

  /**
   * Store indexes for a {@link MetadataEntry}
   *
   * @param targetId the {@link NamespacedEntityId} from which the metadata indexes has to be stored
   * @param metadataKey the metadata key for which the indexes are to be stored
   * @param indexes {@link Set<String>} of indexes to store for this {@link MetadataEntry}
   */
  private void storeIndexes(NamespacedEntityId targetId, String metadataKey, Set<String> indexes) {
    // Delete existing indexes for targetId-key
    deleteIndexes(targetId, metadataKey);

    for (String index : indexes) {
      // store the index with key of the metadata, so that we allow searches of the form [key]:[value]
      indexedTable.put(getIndexPut(targetId, metadataKey, metadataKey + KEYVALUE_SEPARATOR + index));
      // store just the index value
      indexedTable.put(getIndexPut(targetId, metadataKey, index));
    }
  }

  /**
   * Creates a {@link Put} for the a metadata index
   *
   * @param targetId the {@link NamespacedEntityId} from which the metadata index has to be created
   * @param metadataKey the key of the metadata entry
   * @param index the index for this metadata
   * @return {@link Put} which is a index row with the value to be indexed in the {@link #INDEX_COLUMN}
   */
  private Put getIndexPut(NamespacedEntityId targetId, String metadataKey, String index) {
    MDSKey mdsIndexKey = MdsKey.getMDSIndexKey(targetId, metadataKey, index.toLowerCase());
    String namespacedIndex = targetId.getNamespace() + KEYVALUE_SEPARATOR + index.toLowerCase();
    Put put = new Put(mdsIndexKey.getKey());
    put.add(Bytes.toBytes(INDEX_COLUMN), Bytes.toBytes(namespacedIndex));
    return put;
  }

  /**
   * Snapshots the metadata for the given targetId at the given time.
   * @param targetId target id for which metadata needs snapshotting
   */
  private void writeHistory(NamespacedEntityId targetId) {
    Map<String, String> properties = getProperties(targetId);
    Set<String> tags = getTags(targetId);
    Metadata metadata = new Metadata(targetId, properties, tags);
    byte[] row = MdsHistoryKey.getMdsKey(targetId, System.currentTimeMillis()).getKey();
    indexedTable.put(row, Bytes.toBytes(HISTORY_COLUMN), Bytes.toBytes(GSON.toJson(metadata)));
  }

  /**
   * Rebuilds all the indexes in the {@link MetadataDataset} in batches.
   *
   * @param startRowKey the key of the row to start the scan for the current batch with
   * @param limit the batch size
   * @return the row key of the last row scanned in the current batch, {@code null} if there are no more rows to scan.
   */
  @Nullable
  public byte[] rebuildIndexes(@Nullable byte[] startRowKey, int limit) {
    // Now rebuild indexes for all values in the metadata dataset
    byte[] valueRowPrefix = MdsKey.getValueRowPrefix();
    // If startRow is null, start at the beginning, else start at the provided start row
    startRowKey = startRowKey == null ? valueRowPrefix : startRowKey;
    // stopRowKey will always be the last row key with the valueRowPrefix
    byte[] stopRowKey = Bytes.stopKeyForPrefix(valueRowPrefix);
    Row row;
    try (Scanner scanner = indexedTable.scan(startRowKey, stopRowKey)) {
      while ((limit > 0) && (row = scanner.next()) != null) {
        byte[] rowKey = row.getRow();
        String targetType = MdsKey.getTargetType(rowKey);
        NamespacedEntityId namespacedEntityId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
        String metadataKey = MdsKey.getMetadataKey(targetType, rowKey);
        Indexer indexer = getIndexerForKey(metadataKey);
        MetadataEntry metadataEntry = getMetadata(namespacedEntityId, metadataKey);
        if (metadataEntry == null) {
          LOG.warn("Found null metadata entry for a known metadata key {} for entity {} which has an index stored. " +
                     "Ignoring.", metadataKey, namespacedEntityId);
          continue;
        }
        Set<String> indexes = indexer.getIndexes(metadataEntry);
        // storeIndexes deletes old indexes
        storeIndexes(namespacedEntityId, metadataKey, indexes);
        limit--;
      }
      Row startRowForNextBatch = scanner.next();
      if (startRowForNextBatch == null) {
        return null;
      }
      return startRowForNextBatch.getRow();
    }
  }

  /**
   * Delete all indexes in the metadata dataset.
   *
   * @param limit the number of rows (indexes) to delete
   * @return the offset at which to start deletion
   */
  public int deleteAllIndexes(int limit) {
    byte[] indexStartPrefix = MdsKey.getIndexRowPrefix();
    byte[] indexStopPrefix = Bytes.stopKeyForPrefix(indexStartPrefix);
    int count = 0;
    Row row;
    try (Scanner scanner = indexedTable.scan(indexStartPrefix, indexStopPrefix)) {
      while (count < limit && ((row = scanner.next()) != null)) {
        if (deleteIndexRow(row)) {
          count++;
        }
      }
    }
    return count;
  }

  // TODO: CDAP-5663 The entire logic of mapping between Indexers and keys should be made internal to MetadataDataset
  private Indexer getIndexerForKey(String key) {
    if ("schema".equals(key)) {
      return new SchemaIndexer();
    }
    return new DefaultValueIndexer();
  }

  /**
   * Deletes a row if the value in the index column is non-null. This is necessary because at least in the
   * InMemoryTable implementation, after deleting the index row, the index column still has a {@code null} value in it.
   * A {@link Scanner} on the table after the delete returns the deleted rows with {@code null} values.
   *
   * @param row the row to delete
   * @return {@code true} if the row was deleted, {@code false} otherwise
   */
  private boolean deleteIndexRow(Row row) {
    if (row.get(INDEX_COLUMN) == null) {
      return false;
    }
    indexedTable.delete(new Delete(row.getRow()));
    return true;
  }
}
