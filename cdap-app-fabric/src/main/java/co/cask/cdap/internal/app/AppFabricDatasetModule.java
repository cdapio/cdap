/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDatasetDefinition;
import co.cask.cdap.internal.app.runtime.schedule.store.ProgramScheduleStoreDefinition;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;

/**
 * The {@link DatasetModule} for adding dataset definitions defined in app-fabric.
 */
public class AppFabricDatasetModule implements DatasetModule {

  @Override
  public void register(DatasetDefinitionRegistry registry) {
    DatasetDefinition<Table, ? extends DatasetAdmin> tableDef = registry.get(Table.class.getName());
    registry.add(new JobQueueDatasetDefinition(JobQueueDataset.class.getName(), tableDef));

    DatasetDefinition<IndexedTable, ? extends DatasetAdmin> indexedTableDef =
      registry.get(IndexedTable.class.getName());
    registry.add(new ProgramScheduleStoreDefinition(Schedulers.STORE_TYPE_NAME, indexedTableDef));
  }
}
