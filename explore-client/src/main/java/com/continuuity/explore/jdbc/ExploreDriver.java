package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.client.ExternalAsyncExploreClient;
import com.continuuity.explore.service.ExploreException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver.
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

  private static final Pattern CONNECTION_URL_PATTERN = Pattern.compile(ExploreJDBCUtils.URL_PREFIX + ".*");

  // The explore jdbc driver is not JDBC compliant, as tons of functionalities are missing
  private static final boolean JDBC_COMPLIANT = false;

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();
    ExploreClient exploreClient = new ExternalAsyncExploreClient(host, port);
    if (!exploreClient.isAvailable()) {
      throw new SQLException("Cannot connect to {}, service unavailable", url);
    }
    return new ExploreConnection(exploreClient);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return CONNECTION_URL_PATTERN.matcher(url).matches();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    throw new SQLException("Method not supported");
  }

  @Override
  public int getMajorVersion() {
    // TODO implement [REACTOR-319]
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO implement [REACTOR-319]
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }
}
