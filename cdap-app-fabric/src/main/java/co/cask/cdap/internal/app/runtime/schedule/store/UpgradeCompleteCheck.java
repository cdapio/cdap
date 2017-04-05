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

package co.cask.cdap.internal.app.runtime.schedule.store;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.collect.ImmutableList;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
* Checks whether upgrade is complete, and marks the flag when it is done.
*/
public class UpgradeCompleteCheck implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeCompleteCheck.class);

  private final byte[] upgradeKey;
  private final TransactionExecutorFactory factory;
  private final Table table;
  private final AtomicBoolean flag;

  UpgradeCompleteCheck(byte[] upgradeKey, TransactionExecutorFactory factory, Table table, AtomicBoolean flag) {
    this.upgradeKey = upgradeKey;
    this.factory = factory;
    this.table = table;
    this.flag = flag;
  }

  @Override
  public void run() {
    while (!flag.get()) {
      try {
        factory.createExecutor(ImmutableList.of((TransactionAware) table))
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              Row row = table.get(upgradeKey);
              if (!row.isEmpty()) {
                flag.set(true);
              }
            }
          });
      } catch (Exception ex) {
        LOG.debug("UpgradeCompleteCheck Received an exception while trying to read the upgrade flag entry. " +
                    "Will retry in sometime.", ex);
      }

      try {
        TimeUnit.MINUTES.sleep(5);
      } catch (InterruptedException e) {
        LOG.debug("UpgradeCompleteCheck received an interrupt.", e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
