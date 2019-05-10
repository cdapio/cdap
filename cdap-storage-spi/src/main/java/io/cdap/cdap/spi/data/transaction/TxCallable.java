/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.transaction;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;

/**
 * A callable that provides a {@link StructuredTableContext} to programs which may be used to get
 * access to and use tables.
 *
 * @param <V> type of the return value from the {@link #call(StructuredTableContext)}.
 *
 */
@Beta
public interface TxCallable<V> {
  /**
   * Provides a {@link StructuredTableContext} to get instances of {@link StructuredTable}s.
   *
   * <p>
   *   Operations executed on a table within the execution of this method are committed as a single transaction.
   *   The transaction is started before this method is invoked and is committed upon successful execution.
   *   Exceptions thrown while committing the transaction or thrown by user-code result in a rollback of the
   *   transaction.
   * </p>
   *
   * @param context to get tables from
   * @return return value of the call method
   */
  V call(StructuredTableContext context) throws Exception;
}
