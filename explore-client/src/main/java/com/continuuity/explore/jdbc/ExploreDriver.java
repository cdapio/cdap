package com.continuuity.explore.jdbc;

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
    // TODO throw SQLException if explore service is down. Means extracting host and port here and ping service
    return acceptsURL(url) ? new ExploreConnection(url, info) : null;
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
