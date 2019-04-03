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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.utils.ProjectInfo;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
* Checks whether upgrade is complete, and marks the flag when it is done.
*/
public class UpgradeValueLoader extends CacheLoader<byte[], Boolean> {
  public static final byte[] COLUMN = Bytes.toBytes('c');
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeValueLoader.class);
  private static final Logger LIMITED_LOGGER = Loggers.sampling(LOG, LogSamplers.onceEvery(100));

  private final String name;
  private final TransactionExecutorFactory factory;
  private final Table table;
  private final AtomicBoolean resultFlag;

  UpgradeValueLoader(String name, TransactionExecutorFactory factory, Table table) {
    this.name = name;
    this.factory = factory;
    this.table = table;
    this.resultFlag = new AtomicBoolean(false);
  }

  @Override
  public Boolean load(final byte[] key) {
    if (resultFlag.get()) {
      // Result flag is already set, so no need to check the table.
      return true;
    }

    try {
      factory.createExecutor(ImmutableList.of((TransactionAware) table))
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            Row row = table.get(key);
            if (!row.isEmpty()) {
              byte[] value = row.get(COLUMN);
              ProjectInfo.Version actual = new ProjectInfo.Version(Bytes.toString(value));
              if (actual.compareTo(ProjectInfo.getVersion()) >= 0) {
                resultFlag.set(true);
              }
            }
          }
        });
    } catch (Exception ex) {
      LIMITED_LOGGER.debug("Upgrade Check got an exception while trying to read the " +
                             "upgrade version of {} table.", name, ex);
    }
    return resultFlag.get();
  }
}
