/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Unit Tests for SQL Transaction Runner.
 */
public class SqlTransactionRunnerTest {

  @Test(expected = SqlTransactionException.class)
  public void testSqlExceptionPropagation() throws SQLException, TransactionException {
    StructuredTableAdmin mockTableAdmin = Mockito.mock(StructuredTableAdmin.class);
    DataSource mockDataSource = Mockito.mock(DataSource.class);
    Connection mockConnection = Mockito.mock(Connection.class);

    SqlTransactionRunner sqlTransactionRunner = new SqlTransactionRunner(mockTableAdmin,
        mockDataSource, new NoOpMetricsCollectionService(), false, 0);

    Mockito.when(mockDataSource.getConnection()).thenReturn(mockConnection);
    // throw an exception from setTransactionIsolation method
    Mockito.doThrow(new RuntimeException("RuntimeException", new IOException("IOException",
        new SQLException("SQLConnectionException", "08*"))))
        .when(mockConnection).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    // run the test
    sqlTransactionRunner.run(context -> { });
  }

  @Test(expected = SqlTransactionException.class)
  public void testSqlException() throws SQLException, TransactionException {
    StructuredTableAdmin mockTableAdmin = Mockito.mock(StructuredTableAdmin.class);
    DataSource mockDataSource = Mockito.mock(DataSource.class);
    Connection mockConnection = Mockito.mock(Connection.class);

    SqlTransactionRunner sqlTransactionRunner = new SqlTransactionRunner(mockTableAdmin,
        mockDataSource, new NoOpMetricsCollectionService(), false, 0);

    Mockito.when(mockDataSource.getConnection()).thenReturn(mockConnection);
    // throw an exception from setTransactionIsolation method
    Mockito.doThrow(new SQLException("SQLConnectionException", "08*"))
        .when(mockConnection).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    // run the test
    sqlTransactionRunner.run(context -> { });
  }

}
