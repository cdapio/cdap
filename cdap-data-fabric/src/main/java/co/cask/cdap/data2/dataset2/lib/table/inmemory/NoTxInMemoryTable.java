/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.common.conf.CConfiguration;

/**
 * In-memory implementation of Table that does not require transactions.
 */
public class NoTxInMemoryTable extends InMemoryTable {
  public NoTxInMemoryTable(DatasetContext datasetContext, String name, CConfiguration cConf) {
    super(datasetContext, name, cConf);
  }

  @Override
  protected void ensureTransactionIsStarted() {
    // no-op: this does not require a transaction
  }
}
