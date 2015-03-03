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

package co.cask.cdap.explore.service.datasets;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A simple table definition using Datasets2 that is not Record Scannable, to use in tests.
 */
public class NotRecordScannableTableDefinition
  extends AbstractDatasetDefinition<NotRecordScannableTableDefinition.KeyValueTable, DatasetAdmin> {

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public NotRecordScannableTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name);
    Preconditions.checkArgument(tableDef != null, "Table definition is required");
    this.tableDef = tableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
        .properties(properties.getProperties())
        .datasets(tableDef.configure("kv", properties))
        .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("kv"));
  }

  @Override
  public KeyValueTable getDataset(DatasetContext datasetContext, Map<String, String> arguments,
                                  ClassLoader classLoader, DatasetSpecification spec) throws IOException {
    Table table = tableDef.getDataset(datasetContext, arguments, classLoader, spec.getSpecification("kv"));
    return new KeyValueTable(spec.getName(), table);
  }

  /**
   * This class implements a key/value map on top of {@link Table}. Supported
   * operations are read and write.
   */
  public static class KeyValueTable extends AbstractDataset implements
      BatchReadable<byte[], byte[]> {

    // the fixed single column to use for the key
    static final byte[] KEY_COLUMN = { 'c' };

    private final Table table;

    public KeyValueTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    @Nullable
    public byte[] read(String key) {
      return read(Bytes.toBytes(key));
    }

    @Nullable
    public byte[] read(byte[] key) {
      return table.get(key, KEY_COLUMN);
    }


    public void write(String key, String value) {
      this.table.put(Bytes.toBytes(key), KEY_COLUMN, Bytes.toBytes(value));
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public SplitReader<byte[], byte[]> createSplitReader(Split split) {
      return new KeyValueScanner(table.createSplitReader(split));
    }

    public class KeyValueScanner extends SplitReader<byte[], byte[]> {

      // the underlying KeyValueTable's split reader
      private SplitReader<byte[], Row> reader;

      public KeyValueScanner(SplitReader<byte[], Row> reader) {
        this.reader = reader;
      }

      @Override
      public void initialize(Split split) throws InterruptedException {
        this.reader.initialize(split);
      }

      @Override
      public boolean nextKeyValue() throws InterruptedException {
        return this.reader.nextKeyValue();
      }

      @Override
      public byte[] getCurrentKey() throws InterruptedException {
        return this.reader.getCurrentKey();
      }

      @Override
      public byte[] getCurrentValue() throws InterruptedException {
        return this.reader.getCurrentValue().get(KEY_COLUMN);
      }

      @Override
      public void close() {
        this.reader.close();
      }
    }
  }

  /**
   * KeyStructValueTableModule
   */
  public static class NotRecordScannableTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      NotRecordScannableTableDefinition tableDefinition =
          new NotRecordScannableTableDefinition("NotRecordScannableTableDef", table);
      registry.add(tableDefinition);
    }
  }
}
