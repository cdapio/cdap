/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.ProjectInfo;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.FixedAddressExploreClient;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.proto.Id;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver. A proper URL is of the form: jdbc:cdap://<host>:<port>?<param1>=<value1>[&<param2>=<value2>],
 * Where host and port point to CDAP http interface where Explore is enabled, and the additional parameters are from
 * the {@link ExploreConnectionParams.Info} enum.
 */
public class ExploreDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDriver.class);

  static {
    try {
      java.sql.DriverManager.registerDriver(new ExploreDriver());
    } catch (SQLException e) {
      LOG.error("Caught exception when registering CDAP JDBC Driver", e);
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

    ExploreConnectionParams params = ExploreConnectionParams.parseConnectionUrl(url);

    String authToken = getString(params, ExploreConnectionParams.Info.EXPLORE_AUTH_TOKEN, null);
    String namespace = getString(params, ExploreConnectionParams.Info.NAMESPACE, Id.Namespace.DEFAULT.getId());
    boolean sslEnabled = getBoolean(params, ExploreConnectionParams.Info.SSL_ENABLED, false);
    boolean verifySSLCert = getBoolean(params, ExploreConnectionParams.Info.VERIFY_SSL_CERT, true);

    ExploreClient exploreClient =
      new FixedAddressExploreClient(params.getHost(), params.getPort(), authToken, sslEnabled, verifySSLCert);
    try {
      exploreClient.ping();
    } catch (UnauthenticatedException e) {
      throw new SQLException("Cannot connect to " + url + ", not authenticated.");
    } catch (ServiceUnavailableException | ExploreException e) {
      throw new SQLException("Cannot connect to " + url + ", service not available.");
    }
    return new ExploreConnection(exploreClient, namespace, params);
  }

  private String getString(ExploreConnectionParams params, ExploreConnectionParams.Info param, String defaultValue) {
    return Iterables.getFirst(params.getExtraInfos().get(param), defaultValue);
  }

  private boolean getBoolean(ExploreConnectionParams params, ExploreConnectionParams.Info param, boolean defaultValue) {
    String string = getString(params, param, null);
    return string != null ? Boolean.valueOf(string) : defaultValue;
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
    return ProjectInfo.getVersion().getMajor();
  }

  /**
   * Get the minor version number of the Explore driver.
   */
  static int getMinorDriverVersion() {
    return ProjectInfo.getVersion().getMinor();
  }

  /**
   * Get the fix version number of the Explore driver.
   */
  static int getFixDriverVersion() {
    return ProjectInfo.getVersion().getFix();
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

}
