/*
 * Copyright 2014 Continuuity, Inc.
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

import com.google.common.collect.ImmutableMap;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver. A proper URL is of the form: jdbc:reactor://<host>:<port>?<param1>=<value1>[&<param2>=<value2>],
 * Where host and port point to Reactor http interface where Explore is enabled, and the additional parameters are from
 * the {@link com.continuuity.explore.jdbc.ExploreDriver.ConnectionParams.Info} enum.
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

    ConnectionParams params = parseConnectionUrl(url);

    String authToken = null;
    List<String> tokenParams = params.getExtraInfos().get(ConnectionParams.Info.EXPLORE_AUTH_TOKEN);
    if (tokenParams != null && !tokenParams.isEmpty() && !tokenParams.get(0).isEmpty()) {
      authToken = tokenParams.get(0);
    }

    ExploreClient exploreClient = new FixedAddressExploreClient(params.getHost(), params.getPort(), authToken);
    if (!exploreClient.isServiceAvailable()) {
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

  /**
   * Get the major version number of the Explore driver.
   */
  static int getMajorDriverVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 2;
  }

  /**
   * Get the minor version number of the Explore driver.
   */
  static int getMinorDriverVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 4;
  }

  @Override
  public int getMajorVersion() {
    return getMajorDriverVersion();
  }

  @Override
  public int getMinorVersion() {
    return getMinorDriverVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Parse Explore connection url string to retrieve the necessary parameters to connect to Reactor.
   * Package visibility for testing.
   */
  ConnectionParams parseConnectionUrl(String url) {
    // URI does not accept two semicolons in a URL string, hence the substring
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();

    QueryStringDecoder decoder = new QueryStringDecoder(jdbcURI);
    Map<String, List<String>> parameters = decoder.getParameters();
    ImmutableMap.Builder<ConnectionParams.Info, List<String>> builder = ImmutableMap.builder();
    if (parameters != null) {
      for (Map.Entry<String, List<String>> param : parameters.entrySet()) {
        ConnectionParams.Info info = ConnectionParams.Info.fromStr(param.getKey());
        if (info != null) {
          builder.put(info, param.getValue());
        }
      }
    }
    return new ConnectionParams(host, port, builder.build());
  }

  /**
   * Explore connection parameters.
   */
  public static final class ConnectionParams {

    /**
     * Extra Explore connection parameter.
     */
    public enum Info {
      EXPLORE_AUTH_TOKEN("reactor.auth.token");

      private String name;

      private Info(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }

      public static Info fromStr(String name) {
        for (Info info : Info.values()) {
          if (info.getName().equals(name)) {
            return info;
          }
        }
        return null;
      }
    }

    private final String host;
    private final int port;
    private final Map<Info, List<String>> extraInfos;

    private ConnectionParams(String host, int port, Map<Info, List<String>> extraInfos) {
      this.host = host;
      this.port = port;
      this.extraInfos = extraInfos;
    }

    public Map<Info, List<String>> getExtraInfos() {
      return extraInfos;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }
  }
}
