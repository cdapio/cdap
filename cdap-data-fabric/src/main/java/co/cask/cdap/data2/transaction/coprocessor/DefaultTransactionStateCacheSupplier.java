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

package co.cask.cdap.data2.transaction.coprocessor;

import co.cask.tephra.coprocessor.TransactionStateCache;
import co.cask.tephra.coprocessor.TransactionStateCacheSupplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Provides a single shared instance of
 * {@link DefaultTransactionStateCache} for use by transaction
 * coprocessors.
 */
public class DefaultTransactionStateCacheSupplier extends TransactionStateCacheSupplier {
  private final String sysConfigTablePrefix;

  public DefaultTransactionStateCacheSupplier(String sysConfigTablePrefix, Configuration conf) {
    super(conf);
    this.sysConfigTablePrefix = sysConfigTablePrefix;
  }

  /**
   * Returns a singleton instance of the transaction state cache, performing lazy initialization if necessary.
   * @return A shared instance of the transaction state cache.
   */
  @Override
  public TransactionStateCache get() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new DefaultTransactionStateCache(sysConfigTablePrefix);
          instance.setConf(conf);
          instance.start();
        }
      }
    }
    return instance;
  }

}
