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

package io.cdap.cdap.spi.data.sql.jdbc;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import org.apache.commons.pool2.ObjectPool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A metrics data source that will emit metrics about the number of connections.
 */
public class MetricsDataSource implements DataSource {
  private final DataSource dataSource;
  private final MetricsCollectionService metricsCollectionService;
  private final ObjectPool objectPool;

  public MetricsDataSource(DataSource dataSource, MetricsCollectionService metricsCollectionService,
                           ObjectPool objectPool) {
    this.dataSource = dataSource;
    this.metricsCollectionService = metricsCollectionService;
    this.objectPool = objectPool;
  }

  @Override
  public Connection getConnection() throws SQLException {
    MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
    try {
      Connection connection = dataSource.getConnection();
      metricsCollector.gauge(Constants.Metrics.StructuredTable.ACTIVE_CONNECTIONS, objectPool.getNumActive());
      metricsCollector.gauge(Constants.Metrics.StructuredTable.IDLE_CONNECTIONS, objectPool.getNumIdle());
      return connection;
    } catch (SQLException e) {
      metricsCollector.increment(Constants.Metrics.StructuredTable.ERROR_CONNECTIONS, 1L);
      throw e;
    }
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    MetricsContext metricsCollector = metricsCollectionService.getContext(Constants.Metrics.STORAGE_METRICS_TAGS);
    try {
      Connection connection = dataSource.getConnection(username, password);
      metricsCollector.gauge(Constants.Metrics.StructuredTable.ACTIVE_CONNECTIONS, objectPool.getNumActive());
      metricsCollector.gauge(Constants.Metrics.StructuredTable.IDLE_CONNECTIONS, objectPool.getNumIdle());
      return connection;
    } catch (SQLException e) {
      metricsCollector.increment(Constants.Metrics.StructuredTable.ERROR_CONNECTIONS, 1L);
      throw e;
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return dataSource.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return dataSource.isWrapperFor(iface);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return dataSource.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    dataSource.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    dataSource.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return dataSource.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return dataSource.getParentLogger();
  }
}
