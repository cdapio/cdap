/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.coprocessor.hbase96;

import com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCacheSupplier;
import com.continuuity.tephra.coprocessor.TransactionStateCache;
import com.continuuity.tephra.coprocessor.hbase96.TransactionDataJanitor;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Implementation of the {@link com.continuuity.tephra.coprocessor.hbase96.TransactionDataJanitor}
 * coprocessor that uses {@link com.continuuity.data2.transaction.coprocessor.ReactorTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class ReactorTransactionDataJanitor extends TransactionDataJanitor {
  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    String tableName = env.getRegion().getTableDesc().getNameAsString();
    String[] parts = tableName.split("\\.", 2);
    String tableNamespace = "";
    if (parts.length > 0) {
      tableNamespace = parts[0];
    }
    return new ReactorTransactionStateCacheSupplier(tableNamespace, env.getConfiguration());
  }
}
