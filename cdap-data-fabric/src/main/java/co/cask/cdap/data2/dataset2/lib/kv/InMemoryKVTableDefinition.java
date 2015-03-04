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

package co.cask.cdap.data2.dataset2.lib.kv;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * Simple implementation of in-memory non-tx {@link NoTxKeyValueTable}.
 */
public class InMemoryKVTableDefinition extends AbstractDatasetDefinition<NoTxKeyValueTable, DatasetAdmin> {
  private static final Map<String, Map<byte[], byte[]>> tables = Maps.newHashMap();

  public InMemoryKVTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return new DatasetAdminImpl(spec.getName());
  }

  @Override
  public NoTxKeyValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                      Map<String, String> arguments, ClassLoader classLoader) {
    return new InMemoryKVTable(spec.getName());
  }

  private static final class DatasetAdminImpl implements DatasetAdmin {
    private final String tableName;

    private DatasetAdminImpl(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public boolean exists() throws IOException {
      return tables.containsKey(tableName);
    }

    @Override
    public void create() throws IOException {
      tables.put(tableName, new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
    }

    @Override
    public void drop() throws IOException {
      tables.remove(tableName);
    }

    @Override
    public void truncate() throws IOException {
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

  private static final class InMemoryKVTable implements NoTxKeyValueTable {
    private final String tableName;

    private Map<byte[], byte[]> getTable() {
      return tables.get(tableName);
    }

    public InMemoryKVTable(String tableName) {
      Preconditions.checkArgument(tables.containsKey(tableName), "Table does not exist: " + tableName);
      this.tableName = tableName;
    }

    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      if (value == null) {
        getTable().remove(key);
      } else {
        getTable().put(key, value);
      }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
      return getTable().get(key);
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
      registry.add(new InMemoryKVTableDefinition(NoTxKeyValueTable.class.getName()));
    }
  }

}
