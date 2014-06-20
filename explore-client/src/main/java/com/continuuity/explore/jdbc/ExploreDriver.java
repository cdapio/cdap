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
 *
 */
public class ExploreDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDriver.class);
  static {
    try {
      java.sql.DriverManager.registerDriver(new ExploreDriver());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

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
    try {
      if (!exploreClient.isAvailable()) {
        throw new SQLException("Explore is not available on host {}:{}", host, port);
      }
    } catch (ExploreException e) {
      LOG.error("Caught exception when asking for Explore status", e);
      throw new SQLException(e);
    }
    return new ExploreConnection(exploreClient, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches(ExploreJDBCUtils.URL_PREFIX + ".*", url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    // TODO implement
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    // TODO implement
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO implement
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }
}
