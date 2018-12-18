/*
 * Copyright 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.metadata.MetadataConstants;
import co.cask.cdap.data2.metadata.indexer.DefaultValueIndexer;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.data2.metadata.indexer.InvertedTimeIndexer;
import co.cask.cdap.data2.metadata.indexer.InvertedValueIndexer;
import co.cask.cdap.data2.metadata.indexer.MetadataEntityTypeIndexer;
import co.cask.cdap.data2.metadata.indexer.SchemaIndexer;
import co.cask.cdap.data2.metadata.indexer.ValueOnlyIndexer;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/**
 * Dataset that manages Metadata using an {@link IndexedTable}. Supports writing tags and properties. Tags are single
 * elements, whereas properties are key-values.
 *
 * Tags and properties both get translated into a {@link MetadataEntry}. A {@link MetadataEntry} corresponds to a
 * single metadata property or a list of metadata tags that is set on some {@link MetadataEntity},
 * like an application or a namespace. Each entry contains the
 * entity the metadata is for, a key, value, and schema. A metadata property is represented as a MetadataEntry
 * where the key is the property key and the value is the property value. Metadata tags are represented as a
 * MetadataEntry where the key is 'tags' and the value is a comma separated list of the tags.
 *
 * The value for a {@link MetadataEntry} is stored under the 'v' column as a string. The data in the 'v' column
 * is not indexed by the underlying IndexedTable.
 * The row key used to store metadata entries is a composite MDSKey. It always begins with:
 *
 * v:[metadata-entity-type]
 *
 * where metadata-entity-type comes from {@link MetadataEntity#getType()}. Next is a series of pairs that comes from the
 * entity hierarchy. At the very end there is the key from {@link MetadataEntry#getKey()}.
 * For example, suppose we have set two properties and two tags on an entity.
 * The properties are 'owner'='sam' and 'final'='false.
 * The tags are 'small', 'beta'.
 * The entity is mapreduce 'mr' in app 'appX' in namespace 'ns1'.
 *
 * The table will look like:
 *
 * rowkey                                                                           column -> value
 *
 * v:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner         v -> sam
 * v:program:namespace:ns1:application:appX:type:mapreduce:program:mr:final         v -> false
 * v:program:namespace:ns1:application:appX:type:mapreduce:program:mr:tags          v -> small,beta
 *
 * If we set that same metadata on namespace 'ns2', we would be adding rows:
 *
 * v:namespace:namespace:ns2:owner                                                  v -> sam
 * v:namespace:namespace:ns2:final                                                  v -> false
 * v:namespace:namespace:ns2:tags                                                   v -> small,beta
 *
 * Note that in these examples, for readability, ':' is used to separate parts of the MDSKey.
 * In reality, there is no separator. An integer specifying the length of the part is placed in front of each part.
 * This is the write done on every {@link MetadataEntry}.
 * In addition to each entry write, a set of index writes will be done for each entry.
 * Indexes are written to the 'i', 'n', 'in', 'c', and 'ic' columns.
 *
 * The 'i' column is for default metadata indexes
 * The 'n' column is for entity name indexes
 * The 'in' column is for entity name indexes in reverse order
 * The 'c' column is for creation-time indexes
 * The 'ic' column is for creation-time indexes in reverse order
 *
 * Which indexes are used will depend on {@link #getIndexersForKey(String, boolean)}, which essentially
 * looks for special keys that {@link AbstractSystemMetadataWriter} are known to write.
 *
 * Each index will generate one or more index values from the single {@link MetadataEntry} value.
 * For example, suppose an indexer is given a metadata property 'owner'='foo bar', and generates
 * index values 'foo bar', 'owner:foo bar', 'foo', 'owner:foo', 'bar', and 'owner:bar'.
 * That means a single {@link MetadataEntry} for 'owner'='foo bar' will generate six different index values.
 *
 * The rowkey for each index value is similar to the rowkey for the {@link MetadataEntry} except 'i' is used as the
 * prefix instead of 'v', and it includes the index value at the end.
 * Each index value will be written twice.
 * The first write is with the {@link MetadataEntity} namespace as a prefix, used for namespace specific search.
 * The second write is without any prefix, used for cross namespace search. They will be written to different columns.
 * With our previous example, the following data will be written:
 *
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:foo bar       i  -> ns1:foo bar
 *                                                                                        xi -> foo bar
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:foo bar       i  -> ns1:owner:foo bar
 *                                                                                        xi -> owner:foo bar
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:foo           i  -> ns1:foo
 *                                                                                        xi -> foo
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:foo           i  -> ns1:owner:foo
 *                                                                                        xi -> owner:foo
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:bar           i  -> ns1:bar
 *                                                                                        xi -> bar
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:owner:bar           i  -> ns1:owner:bar
 *                                                                                        xi -> owner:bar
 *
 * Since tags are just a special property where the property key is 'tags',
 * if 'foo' and 'bar' are set as a tags on a mapreduce program, the table will look like:
 *
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:tags:foo            i  -> ns1:foo
 *                                                                                        xi -> foo
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:tags:foo            i  -> ns1:tags:foo
 *                                                                                        xi -> tags:foo
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:tags:bar            i  -> ns1:bar
 *                                                                                        xi -> bar
 * i:program:namespace:ns1:application:appX:type:mapreduce:program:mr:tags:bar            i  -> ns1:tags:bar
 *                                                                                        xi -> tags:bar
 *
 * In addition to the entry and it's indexes, there is also history write done per {@link MetadataEntity}.
 * The row key for history is similar to the index row key except it is prefixed by 'h' instead of 'v' and it
 * contains an inverted timestamp at the end instead of the metadata key-value.
 * The timestamp corresponds to the time of the write.
 * The value is stored in the 'h' column and is a {@link Metadata} object.
 * This will give a snapshot of all properties and tags for that entity at the time of that write.
 * For example, for 'owner'='foo' set on a mapreduce program, the history row key will be:
 *
 * h:program:namespace:ns1:application:appX:type:mapreduce:program:mr:[inverted-timestamp]
 *
 * and the value written will contain all properties and tags for that mapreduce program at that time.
 */
public class MetadataDataset extends AbstractDataset {
  /**
   * Type name
   */
  public static final String TYPE = "metadataDataset";
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
    (o1, o2) -> Bytes.compareTo(o1.getFirst(), o2.getFirst());

  private static final Set<Indexer> DEFAULT_INDEXERS = Collections.singleton(new DefaultValueIndexer());
  private static final Map<String, Set<Indexer>> SYSTEM_METADATA_KEY_TO_INDEXERS = ImmutableMap.of(
    AbstractSystemMetadataWriter.SCHEMA_KEY, Collections.singleton(
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

  // default column for metadata indexes
  static final IndexColumn DEFAULT_INDEX_COLUMN = new IndexColumn("i", "xi");
  // column for entity name indexes
  static final IndexColumn ENTITY_NAME_INDEX_COLUMN = new IndexColumn("n", "xn");
  // column for entity name indexes in reverse order
  static final IndexColumn INVERTED_ENTITY_NAME_INDEX_COLUMN = new IndexColumn("in", "xin");
  // column for creation-time indexes
  static final IndexColumn CREATION_TIME_INDEX_COLUMN = new IndexColumn("c", "xc");
  // column for inverted creation-time based index
  static final IndexColumn INVERTED_CREATION_TIME_INDEX_COLUMN = new IndexColumn("ic", "xic");
  static final Collection<IndexColumn> INDEX_COLUMNS =
    ImmutableList.of(DEFAULT_INDEX_COLUMN, ENTITY_NAME_INDEX_COLUMN, INVERTED_ENTITY_NAME_INDEX_COLUMN,
                     CREATION_TIME_INDEX_COLUMN, INVERTED_CREATION_TIME_INDEX_COLUMN);

  private final IndexedTable indexedTable;
  private final MetadataScope scope;

  MetadataDataset(IndexedTable indexedTable, MetadataScope scope) {
    super("metadataDataset", indexedTable);
    this.indexedTable = indexedTable;
    this.scope = scope;
  }

  /**
   * Sets a metadata property for the specified {@link MetadataEntity}.
   * @param metadataEntity the metadata entity for whch the property needs to be updated
   * @param key The metadata key to be added
   * @param value The metadata value to be added
   */
  public MetadataChange setProperty(MetadataEntity metadataEntity, String key, String value) {
    return setMetadata(new MetadataEntry(metadataEntity, key, value));
  }

  /**
   * Adds the given properties the given metadataEntity
   * @param metadataEntity the metadataEntity to which properties needs to be added
   * @param properties the propeties to add (note if the property key exist and new value is different it will be
   * overwritten)
   * @return {@link MetadataChange} representing the change in metadata for the metadataEntity
   */
  public MetadataChange setProperties(MetadataEntity metadataEntity, Map<String, String> properties) {
    Metadata previousMetadata, finalMetadata;
    Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
    // properties can have none, one or more than one entry to be updated
    if (iterator.hasNext()) {
      // if there is at least one entry then we need to process that entry to update metadata and at that point we want
      // to store what was the previousMetadata before we called setMetadata
      Map.Entry<String, String> first = iterator.next();
      MetadataChange metadataChange = setMetadata(new MetadataEntry(metadataEntity, first.getKey(), first.getValue()));
      // metadata before setMetadata was applied
      previousMetadata = metadataChange.getExisting();
      // metadata after setMetadata was applied
      finalMetadata = metadataChange.getLatest();
    } else {
      // if properties was empty then we do need to the existing metadata as it is and also the final state is
      // same as previous state
      previousMetadata = getMetadata(metadataEntity);
      finalMetadata = previousMetadata;
    }
    // if there are more key-value properties then process them updating the final metadata state
    while (iterator.hasNext()) {
      Map.Entry<String, String> next = iterator.next();
      finalMetadata = setMetadata(new MetadataEntry(metadataEntity, next.getKey(), next.getValue())).getExisting();
    }
    return new MetadataChange(previousMetadata, finalMetadata);
  }

  /**
   * Adds a new tag for the specified {@link MetadataEntity}.
   * @param metadataEntity the metadataEntity to which the tag needs to be added
   * @param tagsToAdd the tags to add
   */
  public MetadataChange addTags(MetadataEntity metadataEntity, Set<String> tagsToAdd) {
    return setMetadata(new MetadataEntry(metadataEntity, MetadataConstants.TAGS_KEY,
                                         Joiner.on(TAGS_SEPARATOR).join(tagsToAdd)));
  }

  @VisibleForTesting
  MetadataChange addTags(MetadataEntity entity, String ... tags) {
    return addTags(entity, Sets.newHashSet(tags));
  }

  /**
   * Retrieves all the properties for the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} for which properties are to be retrieved
   * @return the properties of the specified {@link MetadataEntity}
   */
  public Map<String, String> getProperties(MetadataEntity metadataEntity) {
    return getMetadata(metadataEntity).getProperties();
  }

  /**
   * Retrieves all the tags for the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} for which tags are to be retrieved
   * @return the tags of the specified {@link MetadataEntity}
   */
  public Set<String> getTags(MetadataEntity metadataEntity) {
    return getMetadata(metadataEntity).getTags();
  }

  /**
   * Retrieves the metadata for the specified {@link MetadataEntity}.
   *
   * @param metadataEntity the specified {@link MetadataEntity}
   * @return {@link MetadataRecordV2} representing the metadata for the specified {@link MetadataEntity}
   */
  public Metadata getMetadata(MetadataEntity metadataEntity) {
    MDSKey mdsKey = MetadataKey.createValueRowKey(metadataEntity, null);
    byte[] startKey = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);

    Map<String, String> metadata = new HashMap<>();
    try (Scanner scan = indexedTable.scan(startKey, stopKey)) {
      Row next;
      while ((next = scan.next()) != null) {
        String key = MetadataKey.extractMetadataKey(next.getRow());
        byte[] value = next.get(VALUE_COLUMN);
        if (key == null || value == null) {
          continue;
        }
        metadata.put(key, Bytes.toString(value));
      }
    }
    Set<String> tags = splitTags(metadata.get(MetadataConstants.TAGS_KEY));
    // safe to remove since splitTags above copies to new HashSet
    // now we are only left with properties
    metadata.remove(MetadataConstants.TAGS_KEY);
    return new Metadata(metadataEntity, metadata, tags);
  }

  /**
   * Retrieve the {@link MetadataEntry} corresponding to the specified key for the {@link MetadataEntity}.
   *
   * @param metadataEntity the {@link MetadataEntity} for which the {@link MetadataEntry} is to be retrieved
   * @param key the property key for which the {@link MetadataEntry} is to be retrieved
   * @return the {@link MetadataEntry} corresponding to the specified key for the {@link MetadataEntity}
   */
  @Nullable
  public MetadataEntry getProperty(MetadataEntity metadataEntity, String key) {
    return getMetadata(metadataEntity, key);
  }

  /**
   * Return metadata based on target id, and key.
   *
   * @param metadataEntity The id of the target
   * @param key The metadata key to get
   * @return instance of {@link MetadataEntry} for the target type, id, and key
   */
  @Nullable
  private MetadataEntry getMetadata(MetadataEntity metadataEntity, String key) {
    MDSKey mdsKey = MetadataKey.createValueRowKey(metadataEntity, key);
    Row row = indexedTable.get(mdsKey.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(VALUE_COLUMN);
    if (value == null) {
      // This can happen when all tags are removed one by one. The row still exists, but the value is null.
      return null;
    }

    return new MetadataEntry(metadataEntity, key, Bytes.toString(value));
  }

  private Metadata getMetadata(MetadataEntity metadataEntity, Map<String, String> metadata) {
    Map<String, String> properties = new HashMap<>(metadata);
    Set<String> tags = properties.containsKey(MetadataConstants.TAGS_KEY)
      ? splitTags(properties.get(MetadataConstants.TAGS_KEY)) : Collections.emptySet();
    properties.remove(MetadataConstants.TAGS_KEY);
    return new Metadata(metadataEntity, properties, tags);
  }

  private static Set<String> splitTags(@Nullable String tags) {
    if (tags == null) {
      return new HashSet<>();
    }
    Iterable<String> split = Splitter.on(TAGS_SEPARATOR).omitEmptyStrings().trimResults().split(tags);
    return StreamSupport.stream(split.spliterator(), false).collect(Collectors.toSet());
  }

  /**
   * Removes the specified keys from the metadata properties of an entity.
   * @param metadataEntity the {@link MetadataEntity} from which to remove the specified keys
   * @param keys the keys to remove
   */
  public MetadataChange removeProperties(MetadataEntity metadataEntity, Set<String> keys) {
    return removeMetadata(metadataEntity, keys);
  }

  @VisibleForTesting
  void removeProperties(MetadataEntity entity, String ... names) {
    removeProperties(entity, Sets.newHashSet(names));
  }

  /**
   * Removes the specified tags from the specified entity.
   * @param metadataEntity the {@link MetadataEntity} from which to remove the specified tags
   * @param tagsToRemove the tags to remove
   */
  public MetadataChange removeTags(MetadataEntity metadataEntity, Set<String> tagsToRemove) {
    Set<String> existingTags = getTags(metadataEntity);
    if (existingTags.isEmpty()) {
      // nothing to remove
      Metadata emptyMetadata = new Metadata(metadataEntity, Collections.emptyMap(), Collections.emptySet());
      return new MetadataChange(emptyMetadata, emptyMetadata);
    }

    existingTags.removeAll(tagsToRemove);

    // call remove metadata for tags, which will delete all the existing indexes for tags of this metadataEntity
    MetadataChange metadataChange = removeTags(metadataEntity);
    // check if tags are all deleted before set Tags, if tags are all deleted, a null value will be set to the
    // metadataEntity,
    // which will give a NPE later when search.
    if (!existingTags.isEmpty()) {
      MetadataChange metadataChangesOnAdd = addTags(metadataEntity, existingTags);
      return new MetadataChange(metadataChange.getExisting(), metadataChangesOnAdd.getLatest());
    }
    return new MetadataChange(metadataChange.getExisting(), new Metadata(metadataEntity,
                                                                    metadataChange.getExisting().getProperties(),
                                                                    Collections.emptySet()));
  }

  @VisibleForTesting
  MetadataChange removeTags(MetadataEntity entity, String ... tags) {
    return removeTags(entity, Sets.newHashSet(tags));
  }

  /**
   * Removes all properties from the specified entity.
   *
   * @param metadataEntity the {@link MetadataEntity} for which to remove the properties
   */
  public MetadataChange removeProperties(MetadataEntity metadataEntity) {
    return removeMetadata(metadataEntity, input -> !MetadataConstants.TAGS_KEY.equals(input));
  }

  /**
   * Removes all tags from the specified entity.
   *
   * @param metadataEntity the {@link MetadataEntity} for which to remove the tags
   */
  public MetadataChange removeTags(MetadataEntity metadataEntity) {
    return removeMetadata(metadataEntity, MetadataConstants.TAGS_KEY::equals);
  }

  /**
   * Removes all keys that satisfy a given predicate from the metadata of the specified {@link MetadataEntity}.
   * @param metadataEntity the {@link MetadataEntity} for which keys are to be removed
   * @param filter the {@link Predicate} that should be satisfied to remove a key
   */
  private MetadataChange removeMetadata(MetadataEntity metadataEntity, Predicate<String> filter) {
    MDSKey mdsKey = MetadataKey.createValueRowKey(metadataEntity, null);
    byte[] prefix = mdsKey.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    Map<String, String> existingMetadata = new HashMap<>();
    Map<String, String> deletedMetadata = new HashMap<>();

    try (Scanner scan = indexedTable.scan(prefix, stopKey)) {
      Row next;
      while ((next = scan.next()) != null) {
        String value = next.getString(VALUE_COLUMN);
        if (value == null) {
          continue;
        }
        String metadataKey = MetadataKey.extractMetadataKey(next.getRow());

        // put all the metadata for this entity as existing
        existingMetadata.put(metadataKey, value);

        if (filter.test(metadataKey)) {
          // if the key matches the key to be deleted delete it and put it in deleted
          indexedTable.delete(new Delete(next.getRow()));
          // store the key to delete its indexes later
          deletedMetadata.put(metadataKey, value);
        }
      }
    }
    // current metadata is existing - deleted
    Map<String, String> currentMetadata = new HashMap<>(existingMetadata);
    // delete all the indexes for all deleted metadata key
    for (String deletedMetadataKey : deletedMetadata.keySet()) {
      deleteIndexes(metadataEntity, deletedMetadataKey);
      currentMetadata.remove(deletedMetadataKey);
    }

    Metadata changedMetadata = getMetadata(metadataEntity, currentMetadata);
    writeHistory(changedMetadata);
    return new MetadataChange(getMetadata(metadataEntity, existingMetadata), changedMetadata);
  }

  /**
   * Removes all keys for the given metadata
   * @param metadataEntity whose metadata needs to be removed
   * @return {@link MetadataChange} the metadata before and after deletion
   */
  public MetadataChange removeMetadata(MetadataEntity metadataEntity) {
    return removeMetadata(metadataEntity, input -> true);
  }

  /**
   * Removes the specified keys from the metadata of the specified {@link MetadataEntity}.
   * @param metadataEntity the {@link MetadataEntity} for which the specified metadata keys are to be removed
   * @param keys the keys to remove from the metadata of the specified {@link MetadataEntity}
   */
  private MetadataChange removeMetadata(MetadataEntity metadataEntity, Set<String> keys) {
    return removeMetadata(metadataEntity, keys::contains);
  }

  /**
   * Deletes all indexes associated with a metadata key
   *
   * @param metadataEntity the {@link MetadataEntity} for which keys are to be removed
   * @param metadataKey the key to remove from the metadata of the specified {@link MetadataEntity}
   */
  private void deleteIndexes(MetadataEntity metadataEntity, String metadataKey) {
    MDSKey mdsKey = MetadataKey.createIndexRowKey(metadataEntity, metadataKey, null);
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
   * Returns the snapshot of the metadata for entities on or before the given time.
   * @param metadataEntitys entity ids
   * @param timeMillis time in milliseconds
   * @return the snapshot of the metadata for entities on or before the given time
   */
  public Set<Metadata> getSnapshotBeforeTime(Set<MetadataEntity> metadataEntitys, long timeMillis) {
    ImmutableSet.Builder<Metadata> builder = ImmutableSet.builder();
    for (MetadataEntity namespacedEntityId : metadataEntitys) {
      builder.add(getSnapshotBeforeTime(namespacedEntityId, timeMillis));
    }
    return builder.build();
  }

  private Metadata getSnapshotBeforeTime(MetadataEntity metadataEntity, long timeMillis) {
    byte[] scanStartKey = MetadataHistoryKey.getMDSScanStartKey(metadataEntity, timeMillis).getKey();
    byte[] scanEndKey = MetadataHistoryKey.getMDSScanStopKey(metadataEntity).getKey();
    // TODO: add limit to scan, we need only one row
    try (Scanner scanner = indexedTable.scan(scanStartKey, scanEndKey)) {
      Row next = scanner.next();
      if (next != null) {
        return GSON.fromJson(next.getString(HISTORY_COLUMN), Metadata.class);
      } else {
        return new Metadata(metadataEntity);
      }
    }
  }

  /**
   * Returns metadata for a given set of entities
   *
   * @param metadataEntitys entities for which metadata is required
   * @return map of entitiyId to set of metadata for that entity
   */
  public Set<Metadata> getMetadata(Set<? extends MetadataEntity> metadataEntitys) {
    if (metadataEntitys.isEmpty()) {
      return Collections.emptySet();
    }

    List<ImmutablePair<byte [], byte []>> fuzzyKeys = new ArrayList<>(metadataEntitys.size());
    for (MetadataEntity metadataEntity : metadataEntitys) {
      fuzzyKeys.add(getFuzzyKeyFor(metadataEntity));
    }

    // Sort fuzzy keys
    fuzzyKeys.sort(FUZZY_KEY_COMPARATOR);

    // Scan using fuzzy filter. Scan returns one row per property.
    // Group the rows on namespacedId
    Multimap<MetadataEntity, MetadataEntry> metadataMap = HashMultimap.create();
    byte[] start = fuzzyKeys.get(0).getFirst();
    byte[] end = Bytes.stopKeyForPrefix(fuzzyKeys.get(fuzzyKeys.size() - 1).getFirst());
    try (Scanner scan = indexedTable.scan(new Scan(start, end, new FuzzyRowFilter(fuzzyKeys)))) {
      Row next;
      while ((next = scan.next()) != null) {
        MetadataEntry metadataEntry = convertRow(next);
        if (metadataEntry != null) {
          metadataMap.put(metadataEntry.getMetadataEntity(), metadataEntry);
        }
      }
    }

    // Create metadata objects for each entity from grouped rows
    Set<Metadata> metadataSet = new HashSet<>();
    for (Map.Entry<MetadataEntity, Collection<MetadataEntry>> entry : metadataMap.asMap().entrySet()) {
      Map<String, String> properties = new HashMap<>();
      Set<String> tags = Collections.emptySet();
      for (MetadataEntry metadataEntry : entry.getValue()) {
        if (MetadataConstants.TAGS_KEY.equals(metadataEntry.getKey())) {
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
    MetadataEntity metadataEntity = MetadataKey.extractMetadataEntityFromKey(rowKey);
    String key = MetadataKey.extractMetadataKey(rowKey);
    byte[] value = row.get(VALUE_COLUMN);
    if (key == null || value == null) {
      return null;
    }
    return new MetadataEntry(metadataEntity, key, Bytes.toString(value));
  }

  private ImmutablePair<byte[], byte[]> getFuzzyKeyFor(MetadataEntity metadataEntity) {
    // We need to create fuzzy pairs to match the first part of the key containing metadataEntity
    MDSKey mdsKey = MetadataKey.createValueRowKey(metadataEntity, null);
    byte[] keyBytes = mdsKey.getKey();
    // byte array is automatically initialized to 0, which implies fixed match in fuzzy info
    // the row key after metadataEntity doesn't need to be a match.
    // Workaround for HBASE-15676, need to have at least one 1 in the fuzzy filter
    byte[] infoBytes = new byte[keyBytes.length + 1];
    infoBytes[infoBytes.length - 1] = 1;

    // the key array size and mask array size has to be equal so increase the size by 1
    return new ImmutablePair<>(Bytes.concat(keyBytes, new byte[1]), infoBytes);
  }

  /**
   * Searches entities that match the specified search query in the specified namespace and {@link NamespaceId#SYSTEM}
   * for the specified {@link EntityTypeSimpleName}.
   * When using default sorting, limits, cursors, and offset are ignored and all results are returned.
   * When using custom sorting, at most offset + limit * (numCursors + 1) results are returned.
   * When using default sorting, results are returned in whatever order is determined by the underlying storage.
   * When using custom sorting, results are returned sorted according to the field and order specified.
   * When using default sorting, any query is allowed.
   * When using custom sorting, the query must be '*'.
   * In all cases, duplicate entries will be returned if multiple index values point to the same entry.
   * This is often the case when using a '*' query.
   *
   * TODO: (CDAP-13637) clean this up and clearly define a consistent contract
   *
   * @param request the search request
   * @return a {@link SearchResults} object containing a list of {@link MetadataEntry} containing each matching
   *         {@link MetadataEntity} with its associated metadata. It also optionally contains a list of cursors
   *         for subsequent queries to start with, if the specified #sortInfo is not {@link SortInfo#DEFAULT}.
   */
  public SearchResults search(SearchRequest request) throws BadRequestException {
    if (SortInfo.DEFAULT.equals(request.getSortInfo())) {
      return searchByDefaultIndex(request);
    }

    return searchByCustomIndex(request);
  }

  private SearchResults searchByDefaultIndex(SearchRequest request) {
    List<MetadataEntry> results = new LinkedList<>();
    String column = request.isNamespaced() ?
      DEFAULT_INDEX_COLUMN.getColumn() : DEFAULT_INDEX_COLUMN.getCrossNamespaceColumn();

    for (SearchTerm searchTerm : getSearchTerms(request)) {
      Scanner scanner;
      if (searchTerm.isPrefix()) {
        // if prefixed search get start and stop key
        byte[] startKey = Bytes.toBytes(searchTerm.getTerm());
        @SuppressWarnings("ConstantConditions")
        byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
        scanner = indexedTable.scanByIndex(Bytes.toBytes(column), startKey, stopKey);
      } else {
        byte[] value = Bytes.toBytes(searchTerm.getTerm());
        scanner = indexedTable.readByIndex(Bytes.toBytes(column), value);
      }
      try {
        Row next;
        while ((next = scanner.next()) != null) {
          Optional<MetadataEntry> metadataEntry = parseRow(next, column, request.getTypes(),
                                                           request.shouldShowHidden());
          metadataEntry.ifPresent(results::add);
        }
      } finally {
        scanner.close();
      }
    }

    // cursors are currently not supported for default indexes
    return new SearchResults(results, Collections.emptyList());
  }

  private SearchResults searchByCustomIndex(SearchRequest request) throws BadRequestException {
    SortInfo sortInfo = request.getSortInfo();
    int offset = request.getOffset();
    int limit = request.getLimit();
    int numCursors = request.getNumCursors();

    List<MetadataEntry> results = new LinkedList<>();
    IndexColumn indexColumn = getIndexColumn(sortInfo.getSortBy(), sortInfo.getSortOrder());
    String column = request.isNamespaced() ? indexColumn.getColumn() : indexColumn.getCrossNamespaceColumn();
    // we want to return the first chunk of 'limit' elements after offset
    // in addition, we want to pre-fetch 'numCursors' chunks of size 'limit'.
    // Note that there's a potential for overflow so we account by limiting it to Integer.MAX_VALUE
    int fetchSize = (int) Math.min(offset + ((numCursors + 1) * (long) limit), Integer.MAX_VALUE);
    List<String> cursors = new ArrayList<>(numCursors);

    if (!"*".equals(request.getQuery())) {
      throw new BadRequestException("Cannot search with non-default sort with any query other than '*'");
    }

    String cursor = request.getCursor();
    for (SearchTerm searchTerm : getSearchTerms(request)) {
      // start key will be the start key for the namespace, or the start key for the cursor if its defined
      // 'ns1:' for namespace 'ns1' without a cursor, 'ns1:abc' for namespace 'ns1' with cursor 'abc'
      byte[] namespaceStartKey = Bytes.toBytes(searchTerm.getTerm());
      byte[] startKey = namespaceStartKey;
      if (!Strings.isNullOrEmpty(cursor)) {
        String prefix = searchTerm.getNamespaceId() == null ?
          "" : searchTerm.getNamespaceId().getNamespace() + MetadataConstants.KEYVALUE_SEPARATOR;
        startKey = Bytes.toBytes(prefix + cursor);
      }
      @SuppressWarnings("ConstantConditions")
      // stop key should always be the stop key for the namespace, regardless of what the cursor is
      byte[] stopKey = Bytes.stopKeyForPrefix(namespaceStartKey);


      // A cursor is the first element of the a chunk of ordered results. Since its always the first element,
      // we want to add a key as a cursor, if upon dividing the current number of results by the chunk size,
      // the remainder is 1. However, this is not true, when the chunk size is 1, since in that case, the
      // remainder on division can never be 1, it is always 0.
      int mod = (limit == 1) ? 0 : 1;
      try (Scanner scanner = indexedTable.scanByIndex(Bytes.toBytes(column), startKey, stopKey)) {
        Row next;
        while ((next = scanner.next()) != null && results.size() < fetchSize) {
          Optional<MetadataEntry> metadataEntry =
            parseRow(next, column, request.getTypes(), request.shouldShowHidden());
          if (!metadataEntry.isPresent()) {
            continue;
          }
          results.add(metadataEntry.get());

          if (results.size() > limit + offset && (results.size() - offset) % limit == mod) {
            String cursorVal = Bytes.toString(next.get(column));
            // add the cursor, with the namespace removed.
            if (cursorVal != null && request.isNamespaced()) {
              cursorVal = cursorVal.substring(cursorVal.indexOf(MetadataConstants.KEYVALUE_SEPARATOR) + 1);
            }
            cursors.add(cursorVal);
          }
        }
      }
    }
    return new SearchResults(results, cursors);
  }

  // there may not be a MetadataEntry in the row or it may for a different targetType (entityFilter),
  // so return an Optional
  private Optional<MetadataEntry> parseRow(Row rowToProcess, String indexColumn,
                                           Set<EntityTypeSimpleName> entityFilter, boolean showHidden) {
    String rowValue = rowToProcess.getString(indexColumn);
    if (rowValue == null) {
      return Optional.empty();
    }

    final byte[] rowKey = rowToProcess.getRow();
    String targetType = MetadataKey.extractTargetType(rowKey);

    // Filter on target type if not set to include all types
    boolean includeAllTypes = entityFilter.isEmpty() || entityFilter.contains(EntityTypeSimpleName.ALL);
    if (!includeAllTypes && !entityFilter.contains(EntityTypeSimpleName.valueOfSerializedForm(targetType))) {
      return Optional.empty();
    }

    MetadataEntity metadataEntity = MetadataKey.extractMetadataEntityFromKey(rowKey);

    try {
      NamespacedEntityId namespacedEntityId = EntityId.fromMetadataEntity(metadataEntity);
      // if the entity starts with _ then skip it unless the caller choose to showHidden.
      if (!showHidden && namespacedEntityId != null && namespacedEntityId.getEntityName().startsWith("_")) {
        return Optional.empty();
      }
    } catch (IllegalArgumentException e) {
      // ignore. For custom entities we don't really want to hide them if they start with _
    }

    String key = MetadataKey.extractMetadataKey(rowKey);
    MetadataEntry entry = getMetadata(metadataEntity, key);
    return Optional.ofNullable(entry);
  }

  /**
   * Generate the search terms to use for the query.
   * The search query is split on whitespace into one or more raw terms. Each raw term is cleaned and formatted
   * into a SearchTerm.
   * See {@link SearchTerm#from(NamespaceId, String)} for how cleaning and formatting is done.
   *
   * @param searchRequest the request to get search terms for
   * @return formatted search query which is namespaced
   */
  private Iterable<SearchTerm> getSearchTerms(SearchRequest searchRequest) {
    Optional<NamespaceId> namespace = searchRequest.getNamespaceId();
    Set<EntityScope> entityScopes = searchRequest.getEntityScopes();
    String searchQuery = searchRequest.getQuery();
    List<SearchTerm> searchTerms = new LinkedList<>();
    for (String term : Splitter.on(SPACE_SEPARATOR_PATTERN).omitEmptyStrings().trimResults().split(searchQuery)) {
      if (entityScopes.contains(EntityScope.USER)) {
        SearchTerm cleanedTerm = namespace.map(namespaceId -> SearchTerm.from(namespaceId, term))
          .orElseGet(() -> SearchTerm.from(term));
        searchTerms.add(cleanedTerm);
      }
      // for non-system namespaces, also add the system namespace, so entities from system namespace are surfaced
      // in the search results as well
      if (namespace.isPresent() && !NamespaceId.SYSTEM.equals(namespace.get()) &&
        entityScopes.contains(EntityScope.SYSTEM)) {
        searchTerms.add(SearchTerm.from(NamespaceId.SYSTEM, term));
      }
    }
    return searchTerms;
  }

  @VisibleForTesting
  void writeValue(MetadataEntry entry) {
    String key = entry.getKey();
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(entry.getMetadataEntity(), key);
    Put put = new Put(mdsValueKey.getKey());

    // add the metadata value
    put.add(Bytes.toBytes(VALUE_COLUMN), Bytes.toBytes(entry.getValue()));
    indexedTable.put(put);
  }

  /**
   * Store indexes for a {@link MetadataEntry}
   * @param indexers {@link Set<String>} of {@link Indexer indexers} for this {@link MetadataEntry}
   * @param metadataEntry {@link MetadataEntry} for which indexes are to be stored
   */
  void storeIndexes(MetadataEntry metadataEntry, Set<Indexer> indexers) {
    // Delete existing indexes for metadataEntity-key
    deleteIndexes(metadataEntry.getMetadataEntity(), metadataEntry.getKey());

    String namespacePrefix = metadataEntry.getMetadataEntity().getValue(MetadataEntity.NAMESPACE)
      + MetadataConstants.KEYVALUE_SEPARATOR;
    for (Indexer indexer : indexers) {
      Set<String> indexes = indexer.getIndexes(metadataEntry);
      IndexColumn indexColumn = getIndexColumn(metadataEntry.getKey(), indexer.getSortOrder());
      for (String index : indexes) {
        if (index.isEmpty()) {
          continue;
        }
        
        // store one value for within namespace search and one for cross namespace search
        String lowercaseIndex = index.toLowerCase();
        MDSKey mdsIndexKey = MetadataKey.createIndexRowKey(metadataEntry.getMetadataEntity(),
                                                           metadataEntry.getKey(), lowercaseIndex);
        Put put = new Put(mdsIndexKey.getKey());
        put.add(Bytes.toBytes(indexColumn.getCrossNamespaceColumn()), Bytes.toBytes(lowercaseIndex));
        put.add(Bytes.toBytes(indexColumn.getColumn()), Bytes.toBytes(namespacePrefix + lowercaseIndex));
        indexedTable.put(put);
      }
    }
  }

  private IndexColumn getIndexColumn(String key, SortInfo.SortOrder sortOrder) {
    IndexColumn indexColumn = DEFAULT_INDEX_COLUMN;
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
   * Snapshots the metadata for the given metadataEntity at the given time.
   * @param metadata which needs to be snapshot
   */
  private void writeHistory(Metadata metadata) {
    writeHistory(metadata, System.currentTimeMillis());
  }

  private void writeHistory(Metadata metadata, long time) {
    byte[] row = MetadataHistoryKey.getMDSKey(metadata.getMetadataEntity(), time).getKey();
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
    Set<MetadataEntity> previouslySeenEntities = new HashSet<>();
    // Now rebuild indexes for all values in the metadata dataset
    byte[] valueRowPrefix = MetadataKey.getValueRowPrefix();
    // If startRow is null, start at the beginning, else start at the provided start row
    startRowKey = startRowKey == null ? valueRowPrefix : startRowKey;
    // stopRowKey will always be the last row key with the valueRowPrefix
    byte[] stopRowKey = Bytes.stopKeyForPrefix(valueRowPrefix);
    Row row;
    try (Scanner scanner = indexedTable.scan(startRowKey, stopRowKey)) {
      while ((limit > 0) && (row = scanner.next()) != null) {
        byte[] rowKey = row.getRow();
        MetadataEntity metadataEntity = MetadataKey.extractMetadataEntityFromKey(rowKey);
        String metadataKey = MetadataKey.extractMetadataKey(rowKey);
        boolean isNewEntity = previouslySeenEntities.add(metadataEntity);
        Set<Indexer> indexers = getIndexersForKey(metadataKey, isNewEntity);
        MetadataEntry metadataEntry = getMetadata(metadataEntity, metadataKey);
        if (metadataEntry == null) {
          LOG.warn("Found null metadata entry for a known metadata key {} for entity {} which has an index stored. " +
                     "Ignoring.", metadataKey, metadataEntity);
          continue;
        }
        // we also want to index all the entity with its type so in addition to the indexers determined by the key add
        // the type indexer
        indexers.add(new MetadataEntityTypeIndexer());
        // storeIndexes deletes old indexes
        storeIndexes(metadataEntry, indexers);
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
   * Add new key-value pair as metadata
   *
   * @param metadataEntry The value of the metadata to be saved
   * @return {@link MetadataChange} representing the metadata before the change and after
   */
  private MetadataChange setMetadata(MetadataEntry metadataEntry) {
    // get existing metadata
    Metadata existingMetadata = getMetadata(metadataEntry.getMetadataEntity());
    // existingMetadata is a Metadata object containing key-value properties and list of tags. We need to determine if
    // we are writing this entity for the first time in our metadata store as there is some special indexes which we
    // generate for the entity itself and this does not need to be regenerated for every metadata record for a
    // particular entity to minimize writes.
    // We use the current metadata information to determine if we have any record for this entity in the store or not.
    // If we have some properties then we can definitely say we have some metadata record for this
    // entity and hence there is no need for indexing with new entity indexers. We always index with new entity
    // indexers even if there might be some tags as tags update happens as combined action of deleting tags
    // (which will delete all indexes) and rewriting the tag as new record. In case of existing tag an optimization
    // can be made to check if the new metadata being written is properties or tags as in case of properties we will
    // not need include new entity indexes but keep the code cleaner we avoid that conditional check here.
    Set<Indexer> indexersForKey = getIndexersForKey(metadataEntry.getKey(), existingMetadata.getProperties().isEmpty());
    Metadata updatedMetadata = writeWithHistory(existingMetadata, metadataEntry, indexersForKey);
    return new MetadataChange(existingMetadata, updatedMetadata);
  }

  /**
   * Writes the entry for the {@link MetadataEntity}
   * @param existing the existing metadata to which the metadata is being written
   * @param entry the new metadata entry
   * @param indexers the indexers to use for this entry
   * @return {@link} the updated {@link Metadata}
   */
  private Metadata writeWithHistory(Metadata existing, MetadataEntry entry, Set<Indexer> indexers) {
    Metadata updatedMetadata;
    MetadataEntry entryToWrite;
    // Metadata consists of properties and tags. Properties are normal key-value pair and tag is a set of comma
    // separated value whose key is fixes to TAGS_KEY
    if (MetadataConstants.TAGS_KEY.equals(entry.getKey())) {
      // we need to handle tag updates a little differently than normal key-value pair as addition of non existing
      // tag should not overwrite all the other existing tag and also we only want to generate indexes for the
      // newly added tags
      Set<String> updatedTags = new HashSet<>(existing.getTags());
      updatedTags.addAll(splitTags(entry.getValue()));

      // the new metadata entry with updated tags
      entryToWrite = new MetadataEntry(entry.getMetadataEntity(), entry.getKey(),
                                       Joiner.on(TAGS_SEPARATOR).join(updatedTags));
      updatedMetadata = new Metadata(existing.getMetadataEntity(), existing.getProperties(), updatedTags);
    } else {
      // for  properties  key-value pair we just write new value
      entryToWrite = entry;
      HashMap<String, String> updatedProperties = new HashMap<>(existing.getProperties());
      updatedProperties.put(entry.getKey(), entry.getValue());
      updatedMetadata = new Metadata(existing.getMetadataEntity(), updatedProperties, existing.getTags());
    }
    writeValue(entryToWrite);
    // store indexes for the tags being added
    storeIndexes(entry, indexers);
    // snapshot the update
    writeHistory(updatedMetadata);
    return updatedMetadata;
  }

  /**
   * Returns all the {@link Indexer indexers} that apply to a specified metadata key.
   * @param key the metadata key
   * @param isNewEntity whether to include indexers which should be used only if this is the first
   * time a metadata record is being stored for the given metadata entity
   */
    private Set<Indexer> getIndexersForKey(String key, boolean isNewEntity) {
      Set<Indexer> indexers = new HashSet<>();
      // for known keys in system scope, return appropriate indexers
      if (MetadataScope.SYSTEM == scope && SYSTEM_METADATA_KEY_TO_INDEXERS.containsKey(key)) {
        indexers.addAll(SYSTEM_METADATA_KEY_TO_INDEXERS.get(key));
      } else {
        indexers.addAll(DEFAULT_INDEXERS);
      }
      if (isNewEntity) {
        indexers.add(new MetadataEntityTypeIndexer());
      }
      return indexers;
    }

  /**
   * Deletes a row if the value in the index column is non-null. This is necessary because at least in the
   * InMemoryTable implementation, after deleting the index row, the index column still has a {@code null} value in it.
   * A {@link Scanner} on the table after the delete returns the deleted rows with {@code null} values.
   *
   * @param row the row to delete
   */
  private void deleteIndexRow(Row row) {
    for (IndexColumn indexColumn : INDEX_COLUMNS) {
      if (row.get(indexColumn.namespaceColumn) != null || row.get(indexColumn.crossNamespaceColumn) != null) {
        indexedTable.delete(row.getRow());
      }
    }
  }

  @VisibleForTesting
  Scanner searchByIndex(String indexColumn, String value) {
    return indexedTable.readByIndex(Bytes.toBytes(indexColumn), Bytes.toBytes(value));
  }

  /**
   * Columns for an Index.
   */
  static class IndexColumn {
    private final String namespaceColumn;
    private final String crossNamespaceColumn;

    private IndexColumn(String namespaceColumn, String crossNamespaceColumn) {
      this.namespaceColumn = namespaceColumn;
      this.crossNamespaceColumn = crossNamespaceColumn;
    }

    String getColumn() {
      return namespaceColumn;
    }

    String getCrossNamespaceColumn() {
      return crossNamespaceColumn;
    }
  }

  /**
   * Information about a search term.
   */
  static class SearchTerm {
    private final NamespaceId namespaceId;
    private final String term;
    private final boolean isPrefix;

    private SearchTerm(@Nullable NamespaceId namespaceId, String term, boolean isPrefix) {
      this.namespaceId = namespaceId;
      this.term = term;
      this.isPrefix = isPrefix;
    }

    @Nullable
    NamespaceId getNamespaceId() {
      return namespaceId;
    }

    String getTerm() {
      return term;
    }

    boolean isPrefix() {
      return isPrefix;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SearchTerm that = (SearchTerm) o;
      return isPrefix == that.isPrefix && Objects.equals(term, that.term);
    }

    @Override
    public int hashCode() {
      return Objects.hash(term, isPrefix);
    }

    /**
     * Create a SearchTerm. The raw term will be cleaned and formatted to meet the search need.
     * Leading and ending whitespace will be trimmed and characters will be changed to lowercase.
     * If the term is a [key]:[value] search, any whitespace around the ':' separator will be removed.
     * For example, 'foo : bar' will be changed to 'foo:bar'.
     * If it is a prefix search, the part before the ending '*' will be used taken as the term.
     * <p>
     * For example, given raw term ' State : BETA* ', the result will be a prefix search where term = 'state:beta'.
     *
     * @param rawTerm the raw search term
     */
    static SearchTerm from(String rawTerm) {
      return from(null, rawTerm);
    }

    /**
     * Create a SearchTerm. The raw term will be cleaned and formatted to meet the search need.
     * Leading and ending whitespace will be trimmed and characters will be changed to lowercase.
     * If the term is a [key]:[value] search, any whitespace around the ':' separator will be removed.
     * For example, 'foo : bar' will be changed to 'foo:bar'.
     * If it is a prefix search, the part before the ending '*' will be used taken as the term.
     * If it is a namespace search, the namespace will be prefixed to the term.
     * <p>
     * For example, given namespace 'ns1' and raw term ' State : BETA* ', the result will be a
     * prefix search where term = 'ns1:state:beta'.
     *
     * @param namespaceId the namespace id if it is a within-namespace search, or null if it is cross namespace
     * @param rawTerm     the raw search term
     */
    static SearchTerm from(@Nullable NamespaceId namespaceId, String rawTerm) {
      String formattedTerm = rawTerm.trim().toLowerCase();

      if (formattedTerm.contains(MetadataConstants.KEYVALUE_SEPARATOR)) {
        // split the search query in two parts on first occurrence of KEYVALUE_SEPARATOR and the trim the key and value
        String[] split = formattedTerm.split(MetadataConstants.KEYVALUE_SEPARATOR, 2);
        formattedTerm = split[0].trim() + MetadataConstants.KEYVALUE_SEPARATOR + split[1].trim();
      }

      boolean isPrefix = formattedTerm.endsWith("*");
      if (isPrefix) {
        formattedTerm = formattedTerm.substring(0, formattedTerm.lastIndexOf('*'));
      }

      if (namespaceId != null) {
        formattedTerm = namespaceId.getNamespace() + MetadataConstants.KEYVALUE_SEPARATOR + formattedTerm;
      }

      return new SearchTerm(namespaceId, formattedTerm, isPrefix);
    }

  }

}
