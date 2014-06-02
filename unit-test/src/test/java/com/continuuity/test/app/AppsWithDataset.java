package com.continuuity.test.app;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.batch.RowScannable;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitRowScanner;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.data2.dataset2.lib.CompositeDatasetDefinition;
import com.continuuity.internal.data.dataset.Dataset;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.Row;
import com.continuuity.internal.data.dataset.lib.table.Table;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

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
      createDataSet("myTable", "keyValueTable", DatasetInstanceProperties.EMPTY);
      addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoDeploy extends AbstractApplication {
    @Override
    public void configure() {
      createDataSet("myTable", "keyValueTable", DatasetInstanceProperties.EMPTY);
      addDatasetModule("my-kv", KeyValueTableDefinition.Module.class);
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
      table = context.getDataSet("myTable");
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
      super(name, ImmutableMap.of("table", tableDefinition));
    }

    @Override
    public KeyValueTableDefinition.KeyValueTable getDataset(DatasetInstanceSpec spec) throws IOException {
      return new KeyValueTable(spec.getName(), getDataset("table", Table.class, spec));
    }

    /**
     * Custom dataset example: key-value table
     */
    public static class KeyValueTable extends AbstractDataset implements RowScannable<KeyValueTable.Entry> {

      // TODO remove this and use ImmutablePair instead, to test object inspection on parameterized types
      public static class Entry {
        private final String key;
        private final String value;

        public Entry(String key, String value) {
          this.key = key;
          this.value = value;
        }

        public String getKey() {
          return key;
        }

        public String getValue() {
          return value;
        }
      }

      private static final byte[] COL = new byte[0];

      private final Table table;

      public KeyValueTable(String instanceName, Table table) {
        super(instanceName, (Dataset) table);
        this.table = table;
      }

      public void put(String key, String value) throws Exception {
        table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
      }

      public String get(String key) throws Exception {
        return Bytes.toString(table.get(Bytes.toBytes(key), COL));
      }

      @Override
      public Type getRowType() {
        return Entry.class;
      }

      @Override
      public List<Split> getSplits() {
        return table.getSplits();
      }

      @Override
      public SplitRowScanner<Entry> createSplitScanner(Split split) {
        return Scannables.splitRowScanner(
          table.createSplitReader(split),
          new Scannables.RowMaker<byte[], Row, Entry>() {
            @Override
            public Entry makeRow(byte[] key, Row row) {
              return new Entry(Bytes.toString(key), Bytes.toString(row.get(COL)));
            }
          });
      }
    }

    /**
     * Dataset module
     */
    public static class Module implements DatasetModule {
      @Override
      public void register(DatasetDefinitionRegistry registry) {
        DatasetDefinition<Table, DatasetAdmin> tableDefinition = registry.get("table");
        KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", tableDefinition);
        registry.add(keyValueTable);
      }
    }
  }
}

