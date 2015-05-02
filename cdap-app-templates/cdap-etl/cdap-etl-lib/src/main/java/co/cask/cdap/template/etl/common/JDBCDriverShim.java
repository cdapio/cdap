/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.common;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Shim for JDBC driver as a better alternative to mere Class.forName to load the JDBC Driver class.
 *
 * From http://www.kfu.com/~nsayer/Java/dyn-jdbc.html
 * One problem with using <pre>{@code Class.forName()}</pre> to find and load the JDBC Driver class is that it
 * presumes that your driver is in the classpath. This means either packaging the driver in your jar, or having to
 * stick the driver somewhere (probably unpacking it too), or modifying your classpath.
 * But why not use something like URLClassLoader and the overload of Class.forName() that lets you specify the
 * ClassLoader?" Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.
 * The workaround for this is to create a shim class that implements java.sql.Driver.
 * This shim class will do nothing but call the methods of an instance of a JDBC driver that we loaded dynamically.
 *
 * @see ETLDBInputFormat
 * @see ETLDBOutputFormat
 */
public class JDBCDriverShim implements Driver {

  private final Driver delegate;

  public JDBCDriverShim(Driver delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return delegate.acceptsURL(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return delegate.connect(url, info);
  }

  @Override
  public int getMajorVersion() {
    return delegate.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return delegate.getMinorVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return delegate.getPropertyInfo(url, info);
  }

  @Override
  public boolean jdbcCompliant() {
    return delegate.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return delegate.getParentLogger();
  }
}
