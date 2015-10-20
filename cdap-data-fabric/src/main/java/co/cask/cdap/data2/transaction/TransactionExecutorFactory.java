/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction;

import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionExecutor;
import com.google.common.base.Supplier;

/**
 * A factory for transaction executors. In addition to the factory from Tephra, we also need to
 * be able to create an executor that obtains each new transaction context from a supplier. This
 * allows for use of the factory with a {@link DynamicDatasetCache}.
 */
public interface TransactionExecutorFactory extends co.cask.tephra.TransactionExecutorFactory {

  TransactionExecutor createExecutor(Supplier<TransactionContext> txContextSupplier);
}
