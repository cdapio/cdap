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
import co.cask.cdap.data2.metadata.indexer.InvertedTimeIndexer;
import co.cask.cdap.data2.metadata.indexer.InvertedValueIndexer;
import co.cask.cdap.data2.metadata.indexer.SchemaIndexer;
import co.cask.cdap.data2.metadata.indexer.ValueOnlyIndexer;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
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

  private static final Set<Indexer> DEFAULT_INDEXERS = Collections.<Indexer>singleton(new DefaultValueIndexer());
  private static final Map<String, Set<Indexer>> SYSTEM_METADATA_KEY_TO_INDEXERS = ImmutableMap.of(
    AbstractSystemMetadataWriter.SCHEMA_KEY, Collections.<Indexer>singleton(
      new SchemaIndexer()
    ),
    AbstractSystemMetadataWriter.ENTITY_NAME_KEY, ImmutableSet.of(
      // used for listing entities sorted in ascending order of name
      new ValueOnlyIndexer(),
      // used for listing entities sorted in descending order of name
      new InvertedValueIndexer(),
      // used for searching entities with a search query
      new DefaultValueIndexer()
    ),
    AbstractSystemMetadataWriter.CREATION_TIME_KEY, ImmutableSet.of(
      // used for listing entities sorted in ascending order of creation time
      new ValueOnlyIndexer(),
      // used for listing entities sorted in descending order of creation time
      new InvertedTimeIndexer(),
      // used for searching entities with a search query
      new DefaultValueIndexer()
    )
  );

  static final String DEFAULT_INDEX_COLUMN = "i";           // default column for metadata indexes
  static final String ENTITY_NAME_INDEX_COLUMN = "n";       // column for entity name indexes
  static final String INVERTED_ENTITY_NAME_INDEX_COLUMN = "in";    // column for entity name indexes in reverse order
  static final String CREATION_TIME_INDEX_COLUMN = "c";     // column for creation-time indexes
  static final String INVERTED_CREATION_TIME_INDEX_COLUMN = "ic"; // column for inverted creation-time based index

  // TODO: CDAP-7835: This is required to be public only for UpgradeTool
  public static final String COLUMNS_TO_INDEX = Joiner.on(",").join(
    DEFAULT_INDEX_COLUMN,
    ENTITY_NAME_INDEX_COLUMN,
    INVERTED_ENTITY_NAME_INDEX_COLUMN,
    CREATION_TIME_INDEX_COLUMN,
    INVERTED_CREATION_TIME_INDEX_COLUMN
  );

  public static final String TAGS_KEY = "tags";
  public static final String KEYVALUE_SEPARATOR = ":";

  private final IndexedTable indexedTable;
  private final MetadataScope scope;

  MetadataDataset(IndexedTable indexedTable, MetadataScope scope) {
    super("metadataDataset", indexedTable);
    this.indexedTable = indexedTable;
    this.scope = scope;
  }

  /**
   * Add new metadata.
   *
   * @param metadataEntry The value of the metadata to be saved
   */
  private void setMetadata(MetadataEntry metadataEntry) {
    setMetadata(metadataEntry, getIndexersForKey(metadataEntry.getKey()));
  }

  @VisibleForTesting
  void setMetadata(MetadataEntry metadataEntry, Set<Indexer> indexers) {
    NamespacedEntityId targetId = metadataEntry.getTargetId();
    write(targetId, metadataEntry, indexers);
  }

  /**
   * Sets a metadata property for the specified {@link NamespacedEntityId}.
   *
   * @param targetId The target Id: {@link ApplicationId} / {@link ProgramId} / {@link DatasetId}/ {@link StreamId}
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public void setProperty(NamespacedEntityId targetId, String key, String value) {
    setMetadata(new MetadataEntry(targetId, key, value));
  }

  /**
   * Replaces existing tags of the specified {@link NamespacedEntityId} with a new set of tags.
   *
   * @param targetId The target Id: app-id(ns+app) / program-id(ns+app+pgtype+pgm) /
   *                 dataset-id(ns+dataset)/stream-id(ns+stream)
   * @param tags the tags to set
   */
  private void setTags(NamespacedEntityId targetId, String ... tags) {
    setMetadata(new MetadataEntry(targetId, TAGS_KEY, Joiner.on(TAGS_SEPARATOR).join(tags)));
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
    //which will give a NPE later when search.
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
   * @param sortInfo the {@link SortInfo} to sort the results by
   * @param offset index to start with in the search results. To return results from the beginning, pass {@code 0}.
   *               Only applies when #sortInfo is not {@link SortInfo#DEFAULT}
   * @param limit number of results to return, starting from #offset. To return all, pass {@link Integer#MAX_VALUE}.
   *              Only applies when #sortInfo is not {@link SortInfo#DEFAULT}
   * @param numCursors number of cursors to return in the response. A cursor identifies the first index of the
   *                   next page for pagination purposes. Only applies when #sortInfo is not {@link SortInfo#DEFAULT}.
   *                   Defaults to {@code 0}
   * @param cursor the cursor that acts as the starting index for the requested page. This is only applicable when
   *               #sortInfo is not {@link SortInfo#DEFAULT}. If offset is also specified, it is applied starting at
   *               the cursor. If {@code null}, the first row is used as the cursor
   * @param showHidden boolean which specifies whether to display hidden entities (entity whose name start with "_")
   *                    or not.
   * @return a {@link SearchResults} object containing a list of {@link MetadataEntry} containing each matching
   *         {@link NamespacedEntityId} with its associated metadata. It also optionally contains a list of cursors
   *         for subsequent queries to start with, if the specified #sortInfo is not {@link SortInfo#DEFAULT}.
   */
  public SearchResults search(String namespaceId, String searchQuery, Set<MetadataSearchTargetType> types,
                              SortInfo sortInfo, int offset, int limit, int numCursors,
                              @Nullable String cursor, boolean showHidden) {
    if (SortInfo.DEFAULT.equals(sortInfo)) {
      return searchByDefaultIndex(namespaceId, searchQuery, types, showHidden);
    }
    return searchByCustomIndex(namespaceId, types, sortInfo, offset, limit, numCursors, cursor, showHidden);
  }

  private SearchResults searchByDefaultIndex(String namespaceId, String searchQuery,
                                             Set<MetadataSearchTargetType> types, boolean showHidden) {
    List<MetadataEntry> results = new ArrayList<>();
    for (String searchTerm : getSearchTerms(namespaceId, searchQuery)) {
      Scanner scanner;
      if (searchTerm.endsWith("*")) {
        // if prefixed search get start and stop key
        byte[] startKey = Bytes.toBytes(searchTerm.substring(0, searchTerm.lastIndexOf("*")));
        byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
        scanner = indexedTable.scanByIndex(Bytes.toBytes(DEFAULT_INDEX_COLUMN), startKey, stopKey);
      } else {
        byte[] value = Bytes.toBytes(searchTerm);
        scanner = indexedTable.readByIndex(Bytes.toBytes(DEFAULT_INDEX_COLUMN), value);
      }
      try {
        Row next;
        while ((next = scanner.next()) != null) {
          Optional<MetadataEntry> metadataEntry = parseRow(next, DEFAULT_INDEX_COLUMN, types, showHidden);
          if (metadataEntry.isPresent()) {
            results.add(metadataEntry.get());
          }
        }
      } finally {
        scanner.close();
      }
    }
    // cursors are currently not supported for default indexes
    return new SearchResults(results, Collections.<String>emptyList());
  }

  private SearchResults searchByCustomIndex(String namespaceId, Set<MetadataSearchTargetType> types,
                                            SortInfo sortInfo, int offset, int limit, int numCursors,
                                            @Nullable String cursor, boolean showHidden) {
    List<MetadataEntry> results = new ArrayList<>();
    String indexColumn = getIndexColumn(sortInfo.getSortBy(), sortInfo.getSortOrder());
    // we want to return the first chunk of 'limit' elements after offset
    // in addition, we want to pre-fetch 'numCursors' chunks of size 'limit'
    int fetchSize = offset + ((numCursors + 1) * limit);
    List<String> cursors = new ArrayList<>(numCursors);
    for (String searchTerm : getSearchTerms(namespaceId, "*")) {
      byte[] startKey = Bytes.toBytes(searchTerm.substring(0, searchTerm.lastIndexOf("*")));
      byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
      // if a cursor is provided, then start at the cursor
      if (!Strings.isNullOrEmpty(cursor)) {
        String namespaceInStartKey = searchTerm.substring(0, searchTerm.indexOf(KEYVALUE_SEPARATOR));
        startKey = Bytes.toBytes(namespaceInStartKey + KEYVALUE_SEPARATOR + cursor);
      }
      // A cursor is the first element of the a chunk of ordered results. Since its always the first element,
      // we want to add a key as a cursor, if upon dividing the current number of results by the chunk size,
      // the remainder is 1. However, this is not true, when the chunk size is 1, since in that case, the
      // remainder on division can never be 1, it is always 0.
      int mod = limit == 1 ? 0 : 1;
      try (Scanner scanner = indexedTable.scanByIndex(Bytes.toBytes(indexColumn), startKey, stopKey)) {
        Row next;
        int count = 0;
        while ((next = scanner.next()) != null && count < fetchSize) {
          // skip until we reach offset
          if (count < offset) {
            if (parseRow(next, indexColumn, types, showHidden).isPresent()) {
              count++;
            }
            continue;
          }
          Optional<MetadataEntry> metadataEntry = parseRow(next, indexColumn, types, showHidden);
          if (metadataEntry.isPresent()) {
            count++;
            results.add(metadataEntry.get());
          }
          // note that count will be different than results.size, in the case that offset != 0
          if (results.size() % limit == mod && results.size() > limit) {
            // add the cursor, with the namespace removed.
            String cursorWithNamespace = Bytes.toString(next.get(indexColumn));
            cursors.add(cursorWithNamespace.substring(cursorWithNamespace.indexOf(KEYVALUE_SEPARATOR) + 1));
          }
        }
      }
    }
    return new SearchResults(results, cursors);
  }

  // there may not be a MetadataEntry in the row or it may for a different targetType (entityFilter),
  // so return an Optional
  private Optional<MetadataEntry> parseRow(Row rowToProcess, String indexColumn,
                                           Set<MetadataSearchTargetType> entityFilter, boolean showHidden) {
    String rowValue = rowToProcess.getString(indexColumn);
    if (rowValue == null) {
      return Optional.absent();
    }

    final byte[] rowKey = rowToProcess.getRow();
    String targetType = MdsKey.getTargetType(rowKey);

    // Filter on target type if not set to include all types
    boolean includeAllTypes = entityFilter.isEmpty() || entityFilter.contains(MetadataSearchTargetType.ALL);
    if (!includeAllTypes && !entityFilter.contains(MetadataSearchTargetType.valueOfSerializedForm(targetType))) {
      return Optional.absent();
    }

    NamespacedEntityId targetId = MdsKey.getNamespacedIdFromKey(targetType, rowKey);
    // if the entity starts with _ then skip it unless the caller choose to showHidden.
    // This is done to hide entities from Tracker. See: CDAP-7910
    if (!showHidden && targetId.getEntityName().startsWith("_")) {
      return Optional.absent();
    }
    String key = MdsKey.getMetadataKey(targetType, rowKey);
    MetadataEntry entry = getMetadata(targetId, key);
    return Optional.fromNullable(entry);
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
   * @param searchQuery the user specified search query. If {@code *}, returns a singleton list containing
   *                    {code *} which matches everything.
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

  private void write(NamespacedEntityId targetId, MetadataEntry entry, Set<Indexer> indexers) {
    String key = entry.getKey();
    MDSKey mdsValueKey = MdsKey.getMDSValueKey(targetId, key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
    indexedTable.put(put);
    storeIndexes(targetId, key, indexers, entry);
    writeHistory(targetId);
  }

  /**
   * Store indexes for a {@link MetadataEntry}
   *
   * @param targetId the {@link NamespacedEntityId} from which the metadata indexes has to be stored
   * @param metadataKey the metadata key for which the indexes are to be stored
   * @param indexers {@link Set<String>} of {@link Indexer indexers} for this {@link MetadataEntry}
   * @param metadataEntry {@link MetadataEntry} for which indexes are to be stored
   */
  private void storeIndexes(NamespacedEntityId targetId, String metadataKey, Set<Indexer> indexers,
                            MetadataEntry metadataEntry) {
    // Delete existing indexes for targetId-key
    deleteIndexes(targetId, metadataKey);

    for (Indexer indexer : indexers) {
      Set<String> indexes = indexer.getIndexes(metadataEntry);
      String indexColumn = getIndexColumn(metadataKey, indexer.getSortOrder());
      for (String index : indexes) {
        // store just the index value
        indexedTable.put(getIndexPut(targetId, metadataKey, index, indexColumn));
      }
    }
  }

  private String getIndexColumn(String key, SortInfo.SortOrder sortOrder) {
    String indexColumn = DEFAULT_INDEX_COLUMN;
    switch (sortOrder) {
      case ASC:
        switch (key) {
          case AbstractSystemMetadataWriter.ENTITY_NAME_KEY:
            indexColumn = ENTITY_NAME_INDEX_COLUMN;
            break;
          case AbstractSystemMetadataWriter.CREATION_TIME_KEY:
            indexColumn = CREATION_TIME_INDEX_COLUMN;
            break;
        }
        break;
      case DESC:
        switch (key) {
          case AbstractSystemMetadataWriter.ENTITY_NAME_KEY:
            indexColumn = INVERTED_ENTITY_NAME_INDEX_COLUMN;
            break;
          case AbstractSystemMetadataWriter.CREATION_TIME_KEY:
            indexColumn = INVERTED_CREATION_TIME_INDEX_COLUMN;
            break;
        }
        break;
    }
    return indexColumn;
  }

  /**
   * Creates a {@link Put} for the a metadata index
   *
   * @param targetId the {@link NamespacedEntityId} from which the metadata index has to be created
   * @param metadataKey the key of the metadata entry
   * @param index the index for this metadata
   * @param indexColumn the column to store the index in. This column should exist in the
   * {@link IndexedTable#INDEX_COLUMNS_CONF_KEY} property in the dataset's definition
   * @return {@link Put} which is a index row with the value to be indexed in the #indexColumn
   */
  private Put getIndexPut(NamespacedEntityId targetId, String metadataKey, String index, String indexColumn) {
    MDSKey mdsIndexKey = MdsKey.getMDSIndexKey(targetId, metadataKey, index.toLowerCase());
    String namespacedIndex = targetId.getNamespace() + KEYVALUE_SEPARATOR + index.toLowerCase();
    Put put = new Put(mdsIndexKey.getKey());
    put.add(Bytes.toBytes(indexColumn), Bytes.toBytes(namespacedIndex));
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
        Set<Indexer> indexers = getIndexersForKey(metadataKey);
        MetadataEntry metadataEntry = getMetadata(namespacedEntityId, metadataKey);
        if (metadataEntry == null) {
          LOG.warn("Found null metadata entry for a known metadata key {} for entity {} which has an index stored. " +
                     "Ignoring.", metadataKey, namespacedEntityId);
          continue;
        }
        // storeIndexes deletes old indexes
        storeIndexes(namespacedEntityId, metadataKey, indexers, metadataEntry);
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

  /**
   * Returns all the {@link Indexer indexers} that apply to a specified metadata key.
   *
   * @param key the key for which {@link Indexer indexers} are required
   */
  private Set<Indexer> getIndexersForKey(String key) {
    // for known keys in system scope, return appropriate indexers
    if (MetadataScope.SYSTEM == scope && SYSTEM_METADATA_KEY_TO_INDEXERS.containsKey(key)) {
      return SYSTEM_METADATA_KEY_TO_INDEXERS.get(key);
    }
    return DEFAULT_INDEXERS;
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
    if (row.get(DEFAULT_INDEX_COLUMN) == null &&
      row.get(ENTITY_NAME_INDEX_COLUMN) == null &&
      row.get(INVERTED_ENTITY_NAME_INDEX_COLUMN) == null &&
      row.get(CREATION_TIME_INDEX_COLUMN) == null &&
      row.get(INVERTED_CREATION_TIME_INDEX_COLUMN) == null) {
      return false;
    }
    indexedTable.delete(new Delete(row.getRow()));
    return true;
  }

  @VisibleForTesting
  Scanner searchByIndex(String indexColumn, String value) {
    return indexedTable.readByIndex(Bytes.toBytes(indexColumn), Bytes.toBytes(value));
  }
}
