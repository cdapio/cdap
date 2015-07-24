/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Dataset definition with a record scannable table, containing an extensive schema. Used for testing.
 */
public class EmailTableDefinition extends AbstractDatasetDefinition<EmailTableDefinition.EmailTable, DatasetAdmin> {
  private static final Schema SCHEMA = Schema.recordOf("email",
    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("subject", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("sender", Schema.of(Schema.Type.STRING))
  );
  private final DatasetDefinition<? extends Table, ?> tableDef;

  public EmailTableDefinition(String name, DatasetDefinition<? extends Table, ?> orderedTableDefinition) {
    super(name);
    this.tableDef = orderedTableDefinition;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    DatasetProperties propertiesWithSchema = DatasetProperties.builder()
      .addAll(properties.getProperties())
      .add(DatasetProperties.SCHEMA, SCHEMA.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "id")
      .build();
    return DatasetSpecification.builder(instanceName, getName())
      .properties(propertiesWithSchema.getProperties())
      .datasets(tableDef.configure("email-table", propertiesWithSchema))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                               ClassLoader classLoader) throws IOException {
    return tableDef.getAdmin(datasetContext, spec.getSpecification("email-table"), classLoader);
  }

  @Override
  public EmailTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                         Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    Table table = tableDef.getDataset(datasetContext, spec.getSpecification("email-table"), arguments,
                                      classLoader);
    return new EmailTable(spec.getName(), table);
  }

  /**
   * Table that writes emails.
   */
  public static class EmailTable extends AbstractDataset implements RecordScannable<StructuredRecord> {
    private final Table table;

    public EmailTable(String instanceName, Table table) {
      super(instanceName, table);
      this.table = table;
    }

    public void writeEmail(String id, String subject, String body, String sender) {
      Put put = new Put(Bytes.toBytes(id));
      put.add("subject", subject);
      put.add("body", body);
      put.add("sender", sender);
      table.put(put);
    }

    @Override
    public Type getRecordType() {
      return StructuredRecord.class;
    }

    @Override
    public List<Split> getSplits() {
      return table.getSplits();
    }

    @Override
    public RecordScanner<StructuredRecord> createSplitRecordScanner(Split split) {
      return table.createSplitRecordScanner(split);
    }
  }

  /**
   * EmailTable Module
   */
  public static class EmailTableModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      DatasetDefinition<Table, DatasetAdmin> table = registry.get("table");
      EmailTableDefinition emailTable = new EmailTableDefinition("email", table);
      registry.add(emailTable);
    }
  }
}
