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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.transaction.TransactionException;
import co.cask.cdap.spi.data.transaction.TransactionRunner;
import co.cask.cdap.spi.data.transaction.TxRunnable;
import org.apache.tephra.TransactionFailureException;

/**
 * No sql transaction runner to start a transaction
 */
public class NoSqlTransactionRunner implements TransactionRunner {
  private final StructuredTableAdmin tableAdmin;
  private final NamespaceId namespaceId;
  private final Transactional transactional;

  public NoSqlTransactionRunner(StructuredTableAdmin tableAdmin, NamespaceId namespaceId, Transactional transactional) {
    this.tableAdmin = tableAdmin;
    this.namespaceId = namespaceId;
    this.transactional = transactional;
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    try {
      transactional.execute(
        datasetContext -> runnable.run(new NoSqlStructuredTableContext(namespaceId, datasetContext, tableAdmin))
      );
    } catch (TransactionFailureException e) {
      throw new TransactionException("Failure executing NoSql transaction:", e);
    }
  }
}
