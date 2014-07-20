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

package com.continuuity.explore.jdbc;

import com.continuuity.common.conf.Constants;
import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.FixedAddressExploreClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.annotation.meta.param;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver. A proper URL is of the form: jdbc:reactor://<host>:<port>?<param1>=<value1>[;<param2>=<value2>],
 * Where host and port point to Reactor http interface where Explore is enabled, and the additional parameters are from
 * the {@link com.continuuity.explore.jdbc.ExploreJDBCUtils.ConnectionParams.Info} enum.
 */
public class ExploreDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDriver.class);
  static {
    try {
      java.sql.DriverManager.registerDriver(new ExploreDriver());
    } catch (SQLException e) {
      LOG.error("Caught exception when registering Reactor JDBC Driver", e);
    }
  }

  private static final Pattern CONNECTION_URL_PATTERN = Pattern.compile(Constants.Explore.Jdbc.URL_PREFIX + ".*");

  // The explore jdbc driver is not JDBC compliant, as tons of functionalities are missing
  private static final boolean JDBC_COMPLIANT = false;

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    ExploreJDBCUtils.ConnectionParams params = ExploreJDBCUtils.parseConnectionUrl(url);

    ExploreClient exploreClient =
      new FixedAddressExploreClient(params.getHost(), params.getPort(),
                                    params.getExtraInfos().get(
                                      ExploreJDBCUtils.ConnectionParams.Info.EXPLORE_AUTH_TOKEN));
    if (!exploreClient.isAvailable()) {
      throw new SQLException("Cannot connect to " + url + ", service unavailable");
    }
    return new ExploreConnection(exploreClient);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return CONNECTION_URL_PATTERN.matcher(url).matches();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMajorVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 2;
  }

  @Override
  public int getMinorVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 3;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }
}
