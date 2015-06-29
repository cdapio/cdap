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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.dataset2.lib.file.FileSetAdmin;

/**
 * {@link co.cask.cdap.api.dataset.module.DatasetModule}
 * for {@link co.cask.cdap.api.dataset.lib.TimePartitionedFileSet}.
 */
public class TimePartitionedFileSetModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {

    DatasetDefinition<FileSet, FileSetAdmin> fileSetDef = registry.get("fileSet");
    DatasetDefinition<IndexedTable, ? extends DatasetAdmin> indexedTableDef = registry.get("indexedTable");

    // file dataset
    registry.add(new TimePartitionedFileSetDefinition(TimePartitionedFileSet.class.getName(), fileSetDef,
                                                      indexedTableDef));
    registry.add(new TimePartitionedFileSetDefinition("timePartitionedFileSet", fileSetDef, indexedTableDef));
  }
}
