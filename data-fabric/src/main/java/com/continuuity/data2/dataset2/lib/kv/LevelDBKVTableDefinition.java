/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset2.lib.kv;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.data2.dataset.lib.table.leveldb.KeyValue;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.iq80.leveldb.DB;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Simple implementation of in-memory non-tx {@link NoTxKeyValueTable}.
 */
public class LevelDBKVTableDefinition extends AbstractDatasetDefinition<NoTxKeyValueTable, DatasetAdmin> {
  @Inject
  private LevelDBOcTableService service;

  public LevelDBKVTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new DatasetAdminImpl(spec.getName(), service);
  }

  @Override
  public NoTxKeyValueTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new KVTableImpl(spec.getName(), service);
  }

  private static final class DatasetAdminImpl implements DatasetAdmin {
    private final String tableName;
    protected final LevelDBOcTableService service;

    private DatasetAdminImpl(String tableName, LevelDBOcTableService service) throws IOException {
      this.tableName = tableName;
      this.service = service;
    }

    @Override
    public boolean exists() throws IOException {
      try {
        service.getTable(tableName);
        return true;
      } catch (Exception e) {
        return false;
      }
    }

    @Override
    public void create() throws IOException {
      service.ensureTableExists(tableName);
    }

    @Override
    public void drop() throws IOException {
      service.dropTable(tableName);
    }

    @Override
    public void truncate() throws IOException {
      drop();
      create();
    }

    @Override
    public void upgrade() throws IOException {
      // no-op
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  private static final class KVTableImpl implements NoTxKeyValueTable {
    private static final byte[] DATA_COLFAM = Bytes.toBytes("d");
    private static final byte[] DEFAULT_COLUMN = Bytes.toBytes("c");

    private final String tableName;
    private final LevelDBOcTableService service;

    public KVTableImpl(String tableName, LevelDBOcTableService service) throws IOException {
      this.tableName = tableName;
      this.service = service;
    }

    private DB getTable() {
      try {
        return service.getTable(tableName);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      if (value == null) {
        getTable().delete(createKey(key));
      } else {
        getTable().put(createKey(key), value);
      }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
      return getTable().get(createKey(key));
    }

    private static byte[] createKey(byte[] rowKey) {
      return new KeyValue(rowKey, DATA_COLFAM, DEFAULT_COLUMN, 1, KeyValue.Type.Put).getKey();
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }

  /**
   * Registers this type as implementation for {@link NoTxKeyValueTable} using class name.
   */
  public static final class Module implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      registry.add(new LevelDBKVTableDefinition(NoTxKeyValueTable.class.getName()));
    }
  }

}
