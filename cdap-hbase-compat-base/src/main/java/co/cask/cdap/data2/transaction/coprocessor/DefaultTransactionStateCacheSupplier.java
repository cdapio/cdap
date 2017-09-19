/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.coprocessor.TransactionStateCacheSupplier;

/**
 * Provides a single shared instance of
 * {@link DefaultTransactionStateCache} for use by transaction
 * coprocessors.
 */
public class DefaultTransactionStateCacheSupplier extends TransactionStateCacheSupplier {
  public DefaultTransactionStateCacheSupplier(final String tablePrefix,
                                              final CoprocessorEnvironment env) {
    super(new Supplier<TransactionStateCache>() {
      @Override
      public TransactionStateCache get() {
        TransactionStateCache cache = new DefaultTransactionStateCache(tablePrefix, env);
        cache.setConf(env.getConfiguration());
        return cache;
      }
    });
  }
}
