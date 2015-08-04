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

package co.cask.cdap.data2.dataset2.lib.table;


import co.cask.cdap.api.common.Bytes;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDatasetDefinition;
import co.cask.cdap.api.dataset.lib.CompositeDatasetAdmin;
import co.cask.cdap.api.dataset.lib.SnapshotDataset;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SnapshotDefinition extends AbstractDatasetDefinition<SnapshotDataset, DatasetAdmin> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDefinition.class);


  private static final String METADATA_TABLE_NAME = "metadata";
  private static final String MAIN_TABLE_NAME = "maindata";
  private static final String METADATA_PROPERTY_SCHEMA = Schema.recordOf(
    "record",
    Schema.Field.of("version", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("value", Schema.of(Schema.Type.LONG))).toString();
  private static final String METADATA_PROPERTY_ROW_FIELD = "version";
  private static final String METADATA_PROPERTY_COLUMN = "value";

  @Inject
  private TransactionExecutorFactory txExecutorFactory;


  private final DatasetDefinition<? extends Table, ?> metadataTableDef;
  private final DatasetDefinition<? extends Table, ?> mainTableDef;

  /**
   * Ctor that takes in name of this dataset type.
   *
   * @param name this dataset type name
   */
  protected SnapshotDefinition(String name,
                               DatasetDefinition<? extends Table, ?> metadataTableDef,
                               DatasetDefinition<? extends Table, ?> mainTableDef) {
    super(name);
    Preconditions.checkArgument(metadataTableDef != null, "Metadata table definition is required");
    Preconditions.checkArgument(mainTableDef != null, "Main table definition is required");
    this.metadataTableDef = metadataTableDef;
    this.mainTableDef = mainTableDef;
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    LOG.info("Yaojie - configure here");
    DatasetProperties metadataProperties = DatasetProperties.builder()
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, METADATA_PROPERTY_ROW_FIELD)
      .add(Table.PROPERTY_SCHEMA, METADATA_PROPERTY_SCHEMA)
      .build();
    LOG.info("Yaojie - configure here 2 ");
    return DatasetSpecification.builder(instanceName, getName())
      .properties(properties.getProperties())
      .datasets(metadataTableDef.configure(METADATA_TABLE_NAME, metadataProperties),
                mainTableDef.configure(MAIN_TABLE_NAME, properties))
      .build();
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec,
                                ClassLoader classLoader) throws IOException {
    return new CompositeDatasetAdmin(Lists.newArrayList(
      metadataTableDef.getAdmin(datasetContext, spec.getSpecification(METADATA_TABLE_NAME), classLoader),
      mainTableDef.getAdmin(datasetContext, spec.getSpecification(MAIN_TABLE_NAME), classLoader)
    ));
  }

  @Override
  public SnapshotDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader) throws IOException {
    final Table metadataTable = metadataTableDef.getDataset(datasetContext, spec.getSpecification(METADATA_TABLE_NAME),
                                                      arguments, classLoader);
  //    co.cask.cdap.data2.dataset2.tx.DatasetContext metaDatasetContext =
    //    co.cask.cdap.data2.dataset2.tx.DatasetContext.of(metadataTable);
//    Transactional<co.cask.cdap.data2.dataset2.tx.DatasetContext<Table>, Table> transactional =
//      Transactional.of(txExecutorFactory, new Supplier<co.cask.cdap.data2.dataset2.tx.DatasetContext<Table>>() {
//        @Override
//        public co.cask.cdap.data2.dataset2.tx.DatasetContext<Table> get() {
//          try {
//            LOG.info("Yaojie - in the transaction");
//            return co.cask.cdap.data2.dataset2.tx.DatasetContext.of(metadataTable);
//          } catch (Exception e) {
//            LOG.info("Yaojie - get exception");
//            // there's nothing much we can do here
//            throw Throwables.propagate(e);
//          }
//        }
//      });
    Long version = null;
    try {
      version = getVersion(co.cask.cdap.data2.dataset2.tx.DatasetContext.of(metadataTable));
      LOG.info("Yaojie in snapshotDefinition - transaction id is {}", version);
    } catch (Exception e) {
      e.printStackTrace();
    }
    Map<String, String> copyOfArguments = new HashMap<>(arguments);
    // metadataTable.get(Bytes.toBytes(METADATA_PROPERTY_ROW_FIELD)).getLong(Bytes.toBytes(METADATA_PROPERTY_COLUMN));
    copyOfArguments.put(METADATA_PROPERTY_ROW_FIELD, String.valueOf(version));
    Table mainTable = mainTableDef.getDataset(datasetContext, spec.getSpecification(MAIN_TABLE_NAME),
                                              copyOfArguments, classLoader);
    return new SnapshotDataset(spec.getName(), metadataTable, mainTable);
  }

  private Long getVersion(final co.cask.cdap.data2.dataset2.tx.DatasetContext metaDatasetContext) throws Exception {
    Iterable<TransactionAware> txAwares = Iterables.transform(
      Iterables.filter(metaDatasetContext, Predicates.instanceOf(TransactionAware.class)),
      new Function<co.cask.cdap.data2.dataset2.tx.DatasetContext<Table>, TransactionAware>() {
        @Override
        public TransactionAware apply(co.cask.cdap.data2.dataset2.tx.DatasetContext<Table> input) {
          return (TransactionAware) input;
        }
      });
    return txExecutorFactory.createExecutor(txAwares).execute(
      new TransactionExecutor.Function<co.cask.cdap.data2.dataset2.tx.DatasetContext<Table>, Long>() {
        @Override
        public Long apply(co.cask.cdap.data2.dataset2.tx.DatasetContext<Table> ctx) throws Exception {
          Table table = (Table) metaDatasetContext.get();
          return table.get(Bytes.toBytes(METADATA_PROPERTY_ROW_FIELD)).getLong(Bytes.toBytes(METADATA_PROPERTY_COLUMN));
        }
      }, metaDatasetContext);
//    return (Long) transactional.execute(
//      new TransactionExecutor.Function<co.cask.cdap.data2.dataset2.tx.DatasetContext<Table>, Long>() {
//        @Override
//        public Long apply(co.cask.cdap.data2.dataset2.tx.DatasetContext<Table> ctx) throws Exception {
//          Table table = ctx.get();
//          LOG.info("Yaojie - in the transaction");
//          ret table.get(Bytes.toBytes(METADATA_PROPERTY_ROW_FIELD)).getLong(Bytes.toBytes(METADATA_PROPERTY_COLUMN));
//        }
//      });
  }
}
