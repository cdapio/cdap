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
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableAdmin;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableCore;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableDefinition;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.proto.Id;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Factory for creating {@link StreamConsumerStateStore} in level db.
 */
public final class LevelDBStreamConsumerStateStoreFactory implements StreamConsumerStateStoreFactory {
  private final LevelDBTableService tableService;
  private final CConfiguration cConf;

  @Inject
  LevelDBStreamConsumerStateStoreFactory(LevelDBTableService tableService, CConfiguration cConf) {
    this.tableService = tableService;
    this.cConf = cConf;
  }

  @Override
  public synchronized StreamConsumerStateStore create(StreamConfig streamConfig) throws IOException {
    Id.Namespace namespace = streamConfig.getStreamId().getNamespace();
    TableId tableId = StreamUtils.getStateStoreTableId(namespace);

    getLevelDBTableAdmin(tableId).create();
    String levelDBTableName =
      PrefixedNamespaces.namespace(cConf, tableId.getNamespace().getId(), tableId.getTableName());
    LevelDBTableCore coreTable = new LevelDBTableCore(levelDBTableName, tableService);
    return new LevelDBStreamConsumerStateStore(streamConfig, coreTable);
  }

  @Override
  public synchronized void dropAllInNamespace(Id.Namespace namespace) throws IOException {
    TableId tableId = StreamUtils.getStateStoreTableId(namespace);
    getLevelDBTableAdmin(tableId).drop();
  }

  private LevelDBTableAdmin getLevelDBTableAdmin(TableId tableId) throws IOException {
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_COLUMN_FAMILY, "t").build();
    LevelDBTableDefinition tableDefinition = new LevelDBTableDefinition("tableDefinition");
    DatasetSpecification spec = tableDefinition.configure(tableId.getTableName(), props);

    return new LevelDBTableAdmin(DatasetContext.from(tableId.getNamespace().getId()), spec, tableService, cConf);
  }
}
