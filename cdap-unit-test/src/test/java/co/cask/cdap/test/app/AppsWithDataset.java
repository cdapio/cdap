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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AppsWithDataset {
  /**
   *
   */
  public static class AppWithExisting extends AbstractApplication {
    @Override
    public void configure() {
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoCreate extends AbstractApplication {
    @Override
    public void configure() {
      createDataset("myTable", "myKeyValueTable", DatasetProperties.EMPTY);
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoDeploy extends AbstractApplication {
    @Override
    public void configure() {
      createDataset("myTable", "myKeyValueTable", DatasetProperties.EMPTY);
      addDatasetModule("my-kv", KeyValueTableDefinition.Module.class);
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoDeployType extends AbstractApplication {
    @Override
    public void configure() {
      createDataset("myTable", KeyValueTableDefinition.KeyValueTable.class.getName(), DatasetProperties.EMPTY);
      addDatasetType(KeyValueTableDefinition.KeyValueTable.class);
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoDeployTypeShortcut extends AbstractApplication {
    @Override
    public void configure() {
      createDataset("myTable", KeyValueTableDefinition.KeyValueTable.class, DatasetProperties.EMPTY);
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  static class MyProcedure extends AbstractProcedure {
    private KeyValueTableDefinition.KeyValueTable table;

    @Override
    public ProcedureSpecification configure() {
      return ProcedureSpecification.Builder.with()
        .setName(getName())
        .setDescription(getDescription())
        .useDataSet("myTable")
        .build();
    }

    @Override
    public void initialize(ProcedureContext context) {
      super.initialize(context);
      table = context.getDataset("myTable");
    }

    @Handle("set")
    public void set(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      String key = request.getArgument("key");
      String value = request.getArgument("value");
      table.put(key, value);

      responder.sendJson("OK");
    }

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      String key = request.getArgument("key");
      String value = table.get(key);

      responder.sendJson(value);
    }
  }

  /**
   *
   */
  public static class AppUsesAnnotation extends AbstractApplication {
    @Override
    public void configure() {
      addProcedure(new MyProcedureWithUseDataSetAnnotation());
    }
  }

  /**
   *
   */
  static class MyProcedureWithUseDataSetAnnotation extends AbstractProcedure {
    @UseDataSet("myTable")
    private KeyValueTableDefinition.KeyValueTable table;

    @Handle("set")
    public void set(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      String key = request.getArgument("key");
      String value = request.getArgument("value");
      table.put(key, value);

      responder.sendJson("OK");
    }

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      String key = request.getArgument("key");
      String value = table.get(key);

      responder.sendJson(value);
    }
  }

  /**
   * Custom dataset example: key-value table
   */
  static class KeyValueTableDefinition
    extends CompositeDatasetDefinition<KeyValueTableDefinition.KeyValueTable> {

    public KeyValueTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDefinition) {
      super(name, ImmutableMap.of("data", tableDefinition));
    }

    @Override
    public KeyValueTableDefinition.KeyValueTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                                            Map<String, String> arguments,
                                                            ClassLoader classLoader) throws IOException {
      return new KeyValueTable(spec, getDataset(datasetContext, "data", Table.class, spec, arguments, classLoader));
    }

    /**
     * Custom dataset example: key-value table
     */
    public static class KeyValueTable extends AbstractDataset
        implements RecordScannable<ImmutablePair<String, String>> {

      private static final byte[] COL = new byte[0];

      private final Table table;

      public KeyValueTable(DatasetSpecification spec,
                           @EmbeddedDataset("data") Table table) {
        super(spec.getName(), (Dataset) table);
        this.table = table;
      }

      public void put(String key, String value) throws Exception {
        table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
      }

      public String get(String key) throws Exception {
        return Bytes.toString(table.get(Bytes.toBytes(key), COL));
      }

      @Override
      public Type getRecordType() {
        return new TypeToken<ImmutablePair<String, String>>() { }.getType();
      }

      @Override
      public List<Split> getSplits() {
        return table.getSplits();
      }

      @Override
      public RecordScanner<ImmutablePair<String, String>> createSplitRecordScanner(Split split) {
        return Scannables.splitRecordScanner(
            table.createSplitReader(split),
            new Scannables.RecordMaker<byte[], Row, ImmutablePair<String, String>>() {
              @Override
              public ImmutablePair<String, String> makeRecord(byte[] key, Row row) {
                return ImmutablePair.of(Bytes.toString(key), Bytes.toString(row.get(COL)));
              }
            }
        );
      }
    }

    /**
     * Dataset module
     */
    public static class Module implements DatasetModule {
      @Override
      public void register(DatasetDefinitionRegistry registry) {
        DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
        KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("myKeyValueTable", tableDefinition);
        registry.add(keyValueTable);
      }
    }
  }
}

