/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.sql;

import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.sql.SQLException;

/**
 * Test for SQL structured table with retries.
 */
public class SqlStructuredTableRetryTest extends SqlStructuredTableTest {
  private static final String SQL_TRANSACTION_EXCEPTION = "SqlTransactionException";
  private static final String SQL_CONNECTION_EXCEPTION = "SqlConnectionException";
  private static final String TRANSACTION_CONFLICT_SQL_STATE = "40001";
  private static final String CONNECTION_FAILURE_SQL_STATES = "08*";
  private static final String SQL_TRANSACTION_EXCEPTION_MESSAGE = "Mocked Transaction Failure";
  private static final String SQL_CONNECTION_EXCEPTION_MESSAGE = "Mocked Connection Failure";

  @Override
  protected TransactionRunner getTransactionRunner() {
    TransactionRunner transactionRunner = super.getTransactionRunner();
    RetryingSqlTransactionRunner mockedRetryingSqlTransactionRunner =
      (RetryingSqlTransactionRunner) Mockito.spy(transactionRunner);
    SqlTransactionRunner mockedSqlTransactionRunner =
      Mockito.spy(((RetryingSqlTransactionRunner) transactionRunner).getSqlTransactionRunner());

    try {
      Mockito
        .doThrow(
          new SqlTransactionException(SQL_TRANSACTION_EXCEPTION_MESSAGE,
                                      new SQLException(SQL_TRANSACTION_EXCEPTION, TRANSACTION_CONFLICT_SQL_STATE)))
        .doThrow(
          new SqlTransactionException(SQL_CONNECTION_EXCEPTION_MESSAGE,
                                      new SQLException(SQL_CONNECTION_EXCEPTION, CONNECTION_FAILURE_SQL_STATES)))
        .doAnswer(InvocationOnMock::callRealMethod)
        .when(mockedSqlTransactionRunner).run(Mockito.any());
    } catch (TransactionException e) {
      throw new RuntimeException("Test failed while executing transaction", e);
    }

    Mockito.when((mockedRetryingSqlTransactionRunner).getSqlTransactionRunner()).thenReturn(mockedSqlTransactionRunner);
    return mockedRetryingSqlTransactionRunner;
  }
}
