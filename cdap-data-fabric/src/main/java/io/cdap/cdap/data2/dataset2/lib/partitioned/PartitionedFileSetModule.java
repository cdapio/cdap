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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.data2.dataset2.lib.file.FileSetAdmin;

/**
 * {@link io.cdap.cdap.api.dataset.module.DatasetModule} for {@link io.cdap.cdap.api.dataset.lib.PartitionedFileSet}.
 */
public class PartitionedFileSetModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {

    DatasetDefinition<FileSet, FileSetAdmin> fileSetDef = registry.get(FileSet.class.getName());
    DatasetDefinition<IndexedTable, ? extends DatasetAdmin> indexedTableDef =
      registry.get(IndexedTable.class.getName());

    // file dataset
    registry.add(new PartitionedFileSetDefinition(PartitionedFileSet.class.getName(), fileSetDef, indexedTableDef));
    registry.add(new PartitionedFileSetDefinition(PartitionedFileSet.TYPE, fileSetDef, indexedTableDef));
  }
}
