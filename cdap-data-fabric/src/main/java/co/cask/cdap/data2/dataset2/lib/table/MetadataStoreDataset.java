/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
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

  protected <T> T deserialize(byte[] serialized, Type typeOfT) {
    return gson.fromJson(Bytes.toString(serialized), typeOfT);
  }

  public boolean exists(MDSKey id) {
    Row row = table.get(id.getKey());
    if (row.isEmpty()) {
      return false;
    }

    byte[] value = row.get(COLUMN);
    if (value == null) {
      return false;
    }

    return true;
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

    return deserialize(value, typeOfT);
  }

  // returns first that matches
  @Nullable
  public <T> T getFirst(MDSKey id, Type typeOfT) {
    try {
      Scanner scan = table.scan(id.getKey(), Bytes.stopKeyForPrefix(id.getKey()));
      try {
        Row row = scan.next();
        if (row == null || row.isEmpty()) {
          return null;
        }

        byte[] value = row.get(COLUMN);
        if (value == null) {
          return null;
        }

        return deserialize(value, typeOfT);
      } finally {
        scan.close();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // lists all that has same first id parts
  public <T> List<T> list(MDSKey id, Type typeOfT) {
    return list(id, typeOfT, Integer.MAX_VALUE);
  }

  // lists all that has same first id parts, with a limit
  public <T> List<T> list(MDSKey id, Type typeOfT, int limit) {
    return list(id, null, typeOfT, limit, Predicates.<T>alwaysTrue());
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
    return listKV(id, null, typeOfT, limit, Predicates.<T>alwaysTrue());
  }

  // returns mapping of all that has first id parts in range of startId and stopId
  public <T> Map<MDSKey, T> listKV(MDSKey startId, @Nullable MDSKey stopId, Type typeOfT, int limit,
                                   Predicate<T> filter) {
    byte[] startKey = startId.getKey();
    byte[] stopKey = stopId == null ? Bytes.stopKeyForPrefix(startKey) : stopId.getKey();

    Scan scan = new Scan(startKey, stopKey);
    return listKV(scan, typeOfT, limit, filter);
  }

  // returns mapping of all that has first id parts in range of startId and stopId
  private <T> Map<MDSKey, T> listKV(Scan runScan, Type typeOfT, int limit, Predicate<T> filter) {
    try {
      Map<MDSKey, T> map = Maps.newLinkedHashMap();
      Scanner scan = table.scan(runScan);
      try {
        Row next;
        while ((limit > 0) && (next = scan.next()) != null) {
          byte[] columnValue = next.get(COLUMN);
          if (columnValue == null) {
            continue;
          }
          T value = deserialize(columnValue, typeOfT);

          if (filter.apply(value)) {
            MDSKey key = new MDSKey(next.getRow());
            map.put(key, value);
            --limit;
          }
        }
        return map;
      } finally {
        scan.close();
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

  // returns mapping of all that match the given keySet
  public <T> Map<MDSKey, T> listKV(Set<MDSKey> keySet, Type typeOfT, int limit) {

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
    return listKV(scan, typeOfT, limit, Predicates.<T>alwaysTrue());
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

    Scanner scan = table.scan(startKey, stopKey);
    try {
      Row next;
      while ((next = scan.next()) != null) {
        byte[] columnValue = next.get(COLUMN);
        if (columnValue == null) {
          continue;
        }
        T value = deserialize(columnValue, typeOfT);

        MDSKey key = new MDSKey(next.getRow());
        //noinspection ConstantConditions
        if (!function.apply(new KeyValue<>(key, value))) {
          break;
        }
      }
    } finally {
      scan.close();
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
    byte[] prefix = id.getKey();
    byte[] stopKey = Bytes.stopKeyForPrefix(prefix);

    try {
      Scanner scan = table.scan(prefix, stopKey);
      try {
        Row next;
        while ((next = scan.next()) != null) {
          String columnValue = next.getString(COLUMN);
          if (columnValue == null) {
            continue;
          }
          table.delete(new Delete(next.getRow()).add(COLUMN));
        }
      } finally {
        scan.close();
      }
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
    }
    return builder;
  }

  /**
   * Returns a ProgramId given the MDS key
   *
   * @param key the MDS key to be used
   * @return ProgramId created from the MDS key
   */
  protected ProgramId getProgramID(MDSKey key) {
    MDSKey.Splitter splitter = key.split();
    splitter.getString(); // skip recordType
    String namespace = splitter.getString(); // namespaceId
    String application = splitter.getString(); // appId
    splitter.getString(); // skip VersionId
    String type = splitter.getString(); // type
    String program = splitter.getString(); // program
    return (new ProgramId(namespace, application, ProgramType.valueOf(type), program));
  }

}
