package com.continuuity.test.app;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.batch.RecordScannable;
import com.continuuity.api.data.batch.RecordScanner;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetDefinition;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.lib.CompositeDatasetDefinition;
import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
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
      super(name, ImmutableMap.of("data", tableDefinition));
    }

    @Override
    public KeyValueTableDefinition.KeyValueTable getDataset(DatasetSpecification spec,
                                                            ClassLoader classLoader) throws IOException {
      return new KeyValueTable(spec, getDataset("data", Table.class, spec, classLoader));
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

