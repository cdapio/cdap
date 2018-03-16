/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Delete;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Handy dataset to be used for managing metadata
 */
public class MetadataStoreDataset extends AbstractDataset {
  /**
   * All rows we store use single column of this name.
   */
  private static final byte[] COLUMN = Bytes.toBytes("c");

  private final Table table;
  private final Gson gson;

  public MetadataStoreDataset(Table table) {
    this(table, new Gson());
  }

  public MetadataStoreDataset(Table table, Gson gson) {
    super("ignored", table);
    this.table = table;
    this.gson = gson;
  }

  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(gson.toJson(value));
  }

  /**
   * Deserialize the given serialized value of a given type.
   * Default implementation is to use {@link Gson} to deserialize.
   *
   * @param key the key used to fetch the given value
   * @param serialized the serialized value
   * @param typeOfT type of the value
   * @param <T> type of the value object
   * @return the deserialized value
   */
  protected <T> T deserialize(MDSKey key, byte[] serialized, Type typeOfT) {
    return gson.fromJson(Bytes.toString(serialized), typeOfT);
  }

  public boolean exists(MDSKey id) {
    Row row = table.get(id.getKey());
    if (row.isEmpty()) {
      return false;
    }

    byte[] value = row.get(COLUMN);
    return value != null;
  }

  @Nullable
  public <T> T get(MDSKey id, Type typeOfT) {
    Row row = table.get(id.getKey());
    if (row.isEmpty()) {
      return null;
    }

    byte[] value = row.get(COLUMN);
    if (value == null) {
      return null;
    }

    return deserialize(id, value, typeOfT);
  }

  // returns first that matches
  @Nullable
  public <T> T getFirst(MDSKey id, Type typeOfT) {
    try {
      try (Scanner scan = table.scan(id.getKey(), Bytes.stopKeyForPrefix(id.getKey()))) {
        Row row = scan.next();
        if (row == null || row.isEmpty()) {
          return null;
        }

        byte[] value = row.get(COLUMN);
        if (value == null) {
          return null;
        }

        return deserialize(id, value, typeOfT);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Get all non-null values with the given ids
  public <T> List<T> get(Set<MDSKey> ids, Type typeOfT) {
    List<T> resultList = new ArrayList<>();
    List<Get> getList = new ArrayList<>();
    for (MDSKey id : ids) {
      getList.add(new Get(id.getKey()));
    }
    List<Row> rowList = table.get(getList);
    for (Row row : rowList) {
      // This shouldn't fail, otherwise table is breaking contract.
      if (row.isEmpty()) {
        continue;
      }

      byte[] value = row.get(COLUMN);
      if (value == null) {
        continue;
      }
      T result = deserialize(new MDSKey(row.getRow()), value, typeOfT);
      resultList.add(result);
    }
    return resultList;
  }

  // lists all that has same first id parts
  public <T> List<T> list(MDSKey id, Type typeOfT) {
    return list(id, typeOfT, Integer.MAX_VALUE);
  }

  // lists all that has same first id parts, with a limit
  public <T> List<T> list(MDSKey id, Type typeOfT, int limit) {
    return list(id, null, typeOfT, limit, x -> true);
  }

  // lists all that has first id parts in range of startId and stopId
  public <T> List<T> list(MDSKey startId, @Nullable MDSKey stopId, Type typeOfT, int limit,
                          Predicate<T> filter) {
    return Lists.newArrayList(listKV(startId, stopId, typeOfT, limit, filter).values());
  }

  // returns mapping of all that has same first id parts
  public <T> Map<MDSKey, T> listKV(MDSKey id, Type typeOfT) {
    return listKV(id, typeOfT, Integer.MAX_VALUE);
  }

  // returns mapping of  all that has same first id parts, with a limit
  public <T> Map<MDSKey, T> listKV(MDSKey id, Type typeOfT, int limit) {
    return listKV(id, null, typeOfT, limit, x -> true);
  }

  // returns mapping of all that has first id parts in range of startId and stopId
  public <T> Map<MDSKey, T> listKV(MDSKey startId, @Nullable MDSKey stopId, Type typeOfT, int limit,
                                   Predicate<T> filter) {
    return listKV(startId, stopId, typeOfT, limit, null, filter);
  }

  public <T> Map<MDSKey, T> listKV(MDSKey startId, @Nullable MDSKey stopId, Type typeOfT, int limit,
                                   Predicate<MDSKey> keyFilter, Predicate<T> valueFilter) {
    byte[] startKey = startId.getKey();
    byte[] stopKey = stopId == null ? Bytes.stopKeyForPrefix(startKey) : stopId.getKey();

    Scan scan = new Scan(startKey, stopKey);
    return listKV(scan, typeOfT, limit, keyFilter, valueFilter);
  }

  private <T> Map<MDSKey, T> listCombinedFilterKV(Scan runScan, Type typeOfT, int limit,
                                                  @Nullable Predicate<KeyValue<T>> combinedFilter) {
    try {
      Map<MDSKey, T> map = Maps.newLinkedHashMap();
      try (Scanner scan = table.scan(runScan)) {
        Row next;
        while ((limit > 0) && (next = scan.next()) != null) {
          MDSKey key = new MDSKey(next.getRow());
          byte[] columnValue = next.get(COLUMN);
          if (columnValue == null) {
            continue;
          }
          T value = deserialize(key, columnValue, typeOfT);

          KeyValue<T> kv = new KeyValue<>(key, value);
          // Combined Filter doesn't pass
          if (combinedFilter != null && !combinedFilter.test(kv)) {
            continue;
          }

          map.put(kv.getKey(), kv.getValue());
          limit--;
        }
        return map;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private <T> Map<MDSKey, T> listKV(Scan runScan, Type typeOfT, int limit, @Nullable Predicate<MDSKey> keyFilter,
                                    @Nullable Predicate<T> valueFilter) {
    try {
      Map<MDSKey, T> map = Maps.newLinkedHashMap();
      try (Scanner scan = table.scan(runScan)) {
        Row next;
        while ((limit > 0) && (next = scan.next()) != null) {
          MDSKey key = new MDSKey(next.getRow());
          byte[] columnValue = next.get(COLUMN);
          if (columnValue == null) {
            continue;
          }
          T value = deserialize(key, columnValue, typeOfT);

          // Key Filter doesn't pass
          if (keyFilter != null && !keyFilter.test(key)) {
            continue;
          }

          // If Value Filter doesn't pass
          if (valueFilter != null && !valueFilter.test(value)) {
            continue;
          }

          map.put(key, value);
          limit--;
        }
        return map;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // TODO: We should avoid this duplicate code. CDAP-7569.
  private ImmutablePair<byte[], byte[]> getFuzzyKeyFor(MDSKey key) {
    byte[] keyBytes = key.getKey();
    // byte array is automatically initialized to 0, which implies fixed match in fuzzy info
    // the row key after targetId doesn't need to be a match.
    // Workaround for HBASE-15676, need to have at least one 1 in the fuzzy filter
    byte[] infoBytes = new byte[keyBytes.length + 1];
    infoBytes[infoBytes.length - 1] = 1;

    // the key array size and mask array size has to be equal so increase the size by 1
    return new ImmutablePair<>(Bytes.concat(keyBytes, new byte[1]), infoBytes);
  }

  // returns mapping of all that match the given keySet provided they pass the combinedFilter predicate
  public <T> Map<MDSKey, T> listKV(Set<MDSKey> keySet, Type typeOfT, int limit,
                                   @Nullable Predicate<KeyValue<T>> combinedFilter) {
    // Sort fuzzy keys
    List<MDSKey> sortedKeys = Lists.newArrayList(keySet);
    Collections.sort(sortedKeys);

    // Scan using fuzzy filter
    byte[] startKey = sortedKeys.get(0).getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(sortedKeys.get(sortedKeys.size() - 1).getKey());

    List<ImmutablePair<byte [], byte []>> fuzzyKeys = new ArrayList<>();
    for (MDSKey key : sortedKeys) {
      fuzzyKeys.add(getFuzzyKeyFor(key));
    }

    Scan scan = new Scan(startKey, stopKey, new FuzzyRowFilter(fuzzyKeys));
    return listCombinedFilterKV(scan, typeOfT, limit, combinedFilter);
  }

  // returns mapping of all that match the given keySet
  public <T> Map<MDSKey, T> listKV(Set<MDSKey> keySet, Type typeOfT, int limit) {
    return listKV(keySet, typeOfT, limit, x -> true);
  }

  /**
   * Run a scan on MDS.
   *
   * @param startId  scan start key
   * @param stopId   scan stop key
   * @param typeOfT  type of value
   * @param function function to process each element returned from scan.
   *                 If function.apply returns false then the scan is stopped.
   *                 Also, function.apply should not return null.
   * @param <T>      type of value
   */
  public <T> void scan(MDSKey startId, @Nullable MDSKey stopId, Type typeOfT, Function<KeyValue<T>, Boolean> function) {
    byte[] startKey = startId.getKey();
    byte[] stopKey = stopId == null ? Bytes.stopKeyForPrefix(startKey) : stopId.getKey();

    try (Scanner scan = table.scan(startKey, stopKey)) {
      Row next;
      while ((next = scan.next()) != null) {
        byte[] columnValue = next.get(COLUMN);
        if (columnValue == null) {
          continue;
        }
        MDSKey key = new MDSKey(next.getRow());
        T value = deserialize(key, columnValue, typeOfT);

        //noinspection ConstantConditions
        if (!function.apply(new KeyValue<>(key, value))) {
          break;
        }
      }
    }
  }

  /**
   * Output value of a scan.
   *
   * @param <T> Type of scan result object
   */
  public class KeyValue<T> {
    private final MDSKey key;
    private final T value;

    public KeyValue(MDSKey key, T value) {
      this.key = key;
      this.value = value;
    }

    public MDSKey getKey() {
      return key;
    }

    public T getValue() {
      return value;
    }
  }

  public void deleteAll(MDSKey id) {
    deleteAll(id, x -> true);
  }

  public void deleteAll(MDSKey id, @Nullable Predicate<MDSKey> filter) {
    byte[] prefix = id.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    try {
      try (Scanner scan = table.scan(prefix, stopKey)) {
        Row next;
        while ((next = scan.next()) != null) {
          String columnValue = next.getString(COLUMN);
          if (columnValue == null) {
            continue;
          }

          MDSKey key = new MDSKey(next.getRow());
          if (filter != null && !filter.test(key)) {
            continue;
          }
          table.delete(new Delete(next.getRow()).add(COLUMN));
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void delete(MDSKey id) {
    try {
      table.delete(id.getKey(), COLUMN);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public <T> void write(MDSKey id, T value) {
    try {
      table.put(new Put(id.getKey()).add(COLUMN, serialize(value)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected MDSKey.Builder getApplicationKeyBuilder(String recordType, @Nullable ApplicationId applicationId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (applicationId != null) {
      builder.add(applicationId.getNamespace());
      builder.add(applicationId.getApplication());
      builder.add(applicationId.getVersion());
    }
    return builder;
  }

  protected MDSKey.Builder getNamespaceKeyBuilder(String recordType, @Nullable NamespaceId namespaceId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (namespaceId != null) {
      builder.add(namespaceId.getNamespace());
    }
    return builder;
  }

  protected MDSKey.Builder getProgramKeyBuilder(String recordType, @Nullable ProgramId programId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (programId != null) {
      builder.add(programId.getNamespace());
      builder.add(programId.getApplication());
      builder.add(programId.getVersion());
      builder.add(programId.getType().name());
      builder.add(programId.getProgram());
    }
    return builder;
  }

  protected MDSKey.Builder getProgramKeyBuilder(String recordType, @Nullable ProgramRunId programRunId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (programRunId != null) {
      builder.add(programRunId.getNamespace());
      builder.add(programRunId.getApplication());
      builder.add(programRunId.getVersion());
      builder.add(programRunId.getType().name());
      builder.add(programRunId.getProgram());
      builder.add(programRunId.getRun());
    }
    return builder;
  }

  protected MDSKey.Builder getVersionLessProgramKeyBuilder(String recordType, @Nullable ProgramId programId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (programId != null) {
      builder.add(programId.getNamespace());
      builder.add(programId.getApplication());
      builder.add(programId.getType().name());
      builder.add(programId.getProgram());
    }
    return builder;
  }

  protected MDSKey.Builder getVersionLessProgramKeyBuilder(String recordType, @Nullable ProgramRunId programRunId) {
    MDSKey.Builder builder = new MDSKey.Builder().add(recordType);
    if (programRunId != null) {
      builder.add(programRunId.getNamespace());
      builder.add(programRunId.getApplication());
      builder.add(programRunId.getType().name());
      builder.add(programRunId.getProgram());
      builder.add(programRunId.getRun());
    }
    return builder;
  }
}
