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
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Handy dataset to be used for managing metadata
 */
public class MetadataStoreDataset extends AbstractDataset {
  private static final Gson GSON = new Gson();
  /**
   * All rows we store use single column of this name.
   */
  private static final byte[] COLUMN = Bytes.toBytes("c");

  private final Table table;

  public MetadataStoreDataset(Table table) {
    super("ignored", table);
    this.table = table;
  }

  protected <T> byte[] serialize(T value) {
    return Bytes.toBytes(GSON.toJson(value));
  }

  protected <T> T deserialize(byte[] serialized, Class<T> classOfT) {
    return GSON.fromJson(Bytes.toString(serialized), classOfT);
  }

  // returns first that matches
  @Nullable
  public <T> T get(Key id, Class<T> classOfT) {
    try {
      Scanner scan = table.scan(id.getKey(), createStopKey(id.getKey()));
      Row row = scan.next();
      if (row == null || row.isEmpty()) {
        return null;
      }

      byte[] value = row.get(COLUMN);
      if (value == null) {
        return null;
      }

      return deserialize(value, classOfT);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // lists all that has same first id parts
  public <T> List<T> list(Key id, Class<T> classOfT) {
    return list(id, classOfT, Integer.MAX_VALUE);
  }

  // lists all that has same first id parts
  public <T> List<T> list(Key id, Class<T> classOfT, int limit) {
    return list(id, null, classOfT, limit);
  }

  // lists all that has first id parts in range of startId and stopId
  public <T> List<T> list(Key startId, @Nullable Key stopId, Class<T> classOfT, int limit) {
    byte[] startKey = startId.getKey();
    byte[] stopKey = stopId == null ? createStopKey(startKey) : stopId.getKey();

    try {
      List<T> list = Lists.newArrayList();
      Scanner scan = table.scan(startKey, stopKey);
      Row next;
      while ((limit-- > 0) && (next = scan.next()) != null) {
        byte[] columnValue = next.get(COLUMN);
        if (columnValue == null) {
          continue;
        }
        T value = deserialize(columnValue, classOfT);
        list.add(value);
      }
      return list;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public <T> void deleteAll(Key id) {
    byte[] prefix = id.getKey();
    byte[] stopKey = createStopKey(prefix);

    try {
      Scanner scan = table.scan(prefix, stopKey);
      Row next;
      while ((next = scan.next()) != null) {
        String columnValue = next.getString(COLUMN);
        if (columnValue == null) {
          continue;
        }
        table.delete(new Delete(next.getRow()).add(COLUMN));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public <T> void write(Key id, T value) {
    try {
      table.put(new Put(id.getKey()).add(COLUMN, serialize(value)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Metadata entry key
   */
  public static final class Key {
    private final byte[] key;

    private Key(byte[] key) {
      this.key = key;
    }

    public byte[] getKey() {
      return key;
    }

    /**
     * Builds {@link Key}s.
     */
    public static final class Builder {
      private byte[] key;

      public Builder() {
        key = new byte[0];
      }

      public Builder(Key start) {
        this.key = start.getKey();
      }

      public Builder add(String part) {
        byte[] b = Bytes.toBytes(part);
        key = Bytes.add(key, Bytes.toBytes(b.length), b);
        return this;
      }

      public Builder add(String... parts) {
        for (String part : parts) {
          add(part);
        }
        return this;
      }

      public Builder add(long part) {
        key = Bytes.add(key, Bytes.toBytes(part));
        return this;
      }

      public Key build() {
        return new Key(key);
      }
    }
  }

  // NOTE: null means "read to the end"
  @Nullable
  private static byte[] createStopKey(byte[] prefix) {
    for (int i = prefix.length - 1; i >= 0; i--) {
      int unsigned = prefix[i] & 0xff;
      if (unsigned < 0xff) {
        byte[] stopKey = Arrays.copyOf(prefix, i + 1);
        stopKey[stopKey.length - 1]++;
        return stopKey;
      }
    }

    // i.e. "read to the end"
    return null;
  }
}
