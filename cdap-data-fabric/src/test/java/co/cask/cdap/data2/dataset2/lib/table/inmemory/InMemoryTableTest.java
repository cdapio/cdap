/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTableTest;

/**
 *
 */
public class InMemoryTableTest extends BufferingTableTest<InMemoryTable> {

  private static final CConfiguration cConf = CConfiguration.create();

  @Override
  protected InMemoryTable getTable(DatasetContext datasetContext, String name,
                                   ConflictDetection conflictLevel) throws Exception {
    return new InMemoryTable(datasetContext, name, ConflictDetection.valueOf(conflictLevel.name()), cConf);
  }

  @Override
  protected DatasetAdmin getTableAdmin(DatasetContext datasetContext, String name,
                                       DatasetProperties ignored) throws Exception {
    return new InMemoryTableAdmin(datasetContext, name, cConf);
  }
}
