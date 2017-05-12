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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.CompositeDatasetDefinition;
import co.cask.cdap.api.dataset.table.Table;

import java.io.IOException;
import java.util.Map;

/**
 * {@link co.cask.cdap.api.dataset.DatasetDefinition} for {@link JobQueueDataset}.
 */
public class JobQueueDatasetDefinition extends CompositeDatasetDefinition<JobQueueDataset> {

  public JobQueueDatasetDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, JobQueueDataset.EMBEDDED_TABLE_NAME, tableDef);
  }

  @Override
  public JobQueueDataset getDataset(DatasetContext datasetContext, DatasetSpecification spec,
                                    Map<String, String> arguments, ClassLoader classLoader)
    throws IOException {
    Table table = getDataset(
      datasetContext, JobQueueDataset.EMBEDDED_TABLE_NAME, spec, arguments, classLoader);
    return new JobQueueDataset(spec.getName(), table);
  }
}
