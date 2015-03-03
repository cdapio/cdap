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

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
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
import com.google.gson.Gson;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Dataset definition with a record scannable table, containing an extensive schema. Used for testing.
 */
public class ExtensiveSchemaTableDefinition
  extends AbstractDatasetDefinition<ExtensiveSchemaTableDefinition.ExtensiveSchemaTable, DatasetAdmin> {
  private static final Gson GSON = new Gson();

  private final DatasetDefinition<? extends Table, ?> tableDef;

  public ExtensiveSchemaTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(tableDef.configure("ext-schema-table", properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, ClassLoader classLoader,
                               DatasetSpecification spec) throws IOException {
    return tableDef.getAdmin(datasetContext, classLoader, spec.getSpecification("ext-schema-table"));
  }

  @Override
  public ExtensiveSchemaTable getDataset(DatasetSpecification spec,
                                         Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(spec.getSpecification("ext-schema-table"), arguments, classLoader);
    return new ExtensiveSchemaTable(spec.getName(), table);
  }

  /**
   * Extensive schema table.
   */
  public static class ExtensiveSchemaTable extends AbstractDataset implements RecordScannable<ExtensiveSchema> {

    static final byte[] COL = new byte[] {'c', 'o', 'l', '1'};

    private final Table table;

    public ExtensiveSchemaTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void put(String key, ExtensiveSchema value) throws Exception {
      table.put(Bytes.toBytes(key), COL, Bytes.toBytes(GSON.toJson(value)));
    }

    public ExtensiveSchema get(String key) throws Exception {
      return GSON.fromJson(Bytes.toString(table.get(Bytes.toBytes(key), COL)), ExtensiveSchema.class);
    }

    @Override
    public Type getRecordType() {
      return ExtensiveSchema.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<ExtensiveSchema> createSplitRecordScanner(Split split) {
      return Scannables.splitRecordScanner(table.createSplitReader(split), RECORD_MAKER);
    }
  }

  /**
   * Custom Type.
   */
  public static class Value {
    private final String s;
    private final int i;

    public Value(String s, int i) {
      this.s = s;
      this.i = i;
    }
  }

  /**
   * Extensive schema.
   */
  public static class ExtensiveSchema {

    // Primitive types (and string)
    private final String s;
    private final int i;
    private final float f;
    private final double d;
    private final long l;
    private final byte b;
    private final boolean bo;
    private final short sh;

    // Arrays
    private final int[] iArr;
    private final float[] fArr;
    private final double[] dArr;
    private final long[] lArr;
    private final byte[] bArr;
    private final boolean[] boArr;
    private final short[] shArr;
    private final String[] sArr;

    // Lists
    private final List<Integer> iList;
    private final List<Float> fList;
    private final List<Double> dList;
    private final List<Long> lList;
    private final List<Byte> bList;
    private final List<Boolean> boList;
    private final List<Short> shList;
    private final List<String> sList;

    // Maps
    private final Map<String, Integer> stoiMap;
    private final Map<Float, Double> ftodMap;
    private final Map<Long, Byte> ltobMap;
    private final Map<Boolean, Short> botoshMap;

    // Custom type
    private final Value v;
    private final Value[] vArr;
    private final List<Value> vList;
    private final Map<String, Value> stovMap;

    // Transient and static fields - they shouldn't be included in the schema
    private transient int t = 0;
    private static int st = 80;

    // Reference to itself
    // TODO fix infinite loop
//    private ExtensiveSchema ext;

    public ExtensiveSchema(String s, int i, float f, double d, long l, byte b, boolean bo, short sh, int[] iArr,
                           float[] fArr, double[] dArr, long[] lArr, byte[] bArr, boolean[] boArr, short[] shArr,
                           String[] sArr, List<Integer> iList, List<Float> fList, List<Double> dList, List<Long> lList,
                           List<Byte> bList, List<Boolean> boList, List<Short> shList, List<String> sList,
                           Map<String, Integer> stoiMap, Map<Float, Double> ftodMap, Map<Long, Byte> ltobMap,
                           Map<Boolean, Short> botoshMap, Value v, Value[] vArr, List<Value> vList,
                           Map<String, Value> stovMap) {
      this.s = s;
      this.i = i;
      this.f = f;
      this.d = d;
      this.l = l;
      this.b = b;
      this.bo = bo;
      this.sh = sh;
      this.iArr = iArr;
      this.fArr = fArr;
      this.dArr = dArr;
      this.lArr = lArr;
      this.bArr = bArr;
      this.boArr = boArr;
      this.shArr = shArr;
      this.sArr = sArr;
      this.iList = iList;
      this.fList = fList;
      this.dList = dList;
      this.lList = lList;
      this.bList = bList;
      this.boList = boList;
      this.shList = shList;
      this.sList = sList;
      this.stoiMap = stoiMap;
      this.ftodMap = ftodMap;
      this.ltobMap = ltobMap;
      this.botoshMap = botoshMap;
      this.v = v;
      this.vArr = vArr;
      this.vList = vList;
      this.stovMap = stovMap;
    }

    public void setExt(ExtensiveSchema ext) {
//      this.ext = ext;
    }
  }

  /**
   * ExtensiveSchemaTableModule.
   */
  public static class ExtensiveSchemaTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      ExtensiveSchemaTableDefinition extensiveSchemaTable =
        new ExtensiveSchemaTableDefinition("ExtensiveSchemaTable", table);
      registry.add(extensiveSchemaTable);
    }
  }

  private static final Scannables.RecordMaker<byte[], Row, ExtensiveSchema> RECORD_MAKER =
    new Scannables.RecordMaker<byte[], Row, ExtensiveSchema>() {
      @Override
      public ExtensiveSchema makeRecord(byte[] key, Row row) {
        return GSON.fromJson(Bytes.toString(row.get(ExtensiveSchemaTable.COL)), ExtensiveSchema.class);
      }
    };
}
