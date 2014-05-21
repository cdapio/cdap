package com.continuuity.test.app;

import com.continuuity.api.AbstractApplication;
import com.continuuity.api.ApplicationConfigurer;
import com.continuuity.api.ApplicationContext;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.data2.dataset2.lib.AbstractDataset;
import com.continuuity.data2.dataset2.lib.CompositeDatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;
import com.continuuity.internal.data.dataset.lib.table.OrderedTable;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 *
 */
public class AppsWithDataset {
  /**
   *
   */
  public static class AppWithExisting extends AbstractApplication {
    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      configurer.addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoCreate extends AbstractApplication {
    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      configurer.addDataSet("myTable", "keyValueTable", DatasetInstanceProperties.EMPTY);
      configurer.addProcedure(new MyProcedure());
    }
  }

  /**
   *
   */
  public static class AppWithAutoDeploy extends AbstractApplication {
    @Override
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      configurer.addDataSet("myTable", "keyValueTable", DatasetInstanceProperties.EMPTY);
      configurer.addDatasetModule("my-kv", KeyValueTableDefinition.Module.class);
      configurer.addProcedure(new MyProcedure());
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
    public void configure(ApplicationConfigurer configurer, ApplicationContext context) {
      configurer.addProcedure(new MyProcedureWithUseDataSetAnnotation());
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

    public KeyValueTableDefinition(String name, DatasetDefinition<? extends OrderedTable, ?> orderedTableDefinition) {
      super(name, ImmutableMap.of("table", orderedTableDefinition));
    }

    @Override
    public KeyValueTableDefinition.KeyValueTable getDataset(DatasetInstanceSpec spec) throws IOException {
      return new KeyValueTable(spec.getName(), getDataset("table", OrderedTable.class, spec));
    }

    /**
     * Custom dataset example: key-value table
     */
    public static class KeyValueTable extends AbstractDataset {
      private static final byte[] COL = new byte[0];

      private final OrderedTable table;

      public KeyValueTable(String instanceName, OrderedTable table) {
        super(instanceName, table);
        this.table = table;
      }

      public void put(String key, String value) throws Exception {
        table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
      }

      public String get(String key) throws Exception {
        return Bytes.toString(table.get(Bytes.toBytes(key), COL));
      }
    }

    /**
     * Dataset module
     */
    public static class Module implements DatasetModule {
      @Override
      public void register(DatasetDefinitionRegistry registry) {
        DatasetDefinition orderedTable = registry.get("orderedTable");
        KeyValueTableDefinition keyValueTable = new KeyValueTableDefinition("keyValueTable", orderedTable);
        registry.add(keyValueTable);
      }
    }
  }
}

