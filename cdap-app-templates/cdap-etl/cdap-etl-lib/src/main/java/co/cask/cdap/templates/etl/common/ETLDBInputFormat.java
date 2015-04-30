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

package co.cask.cdap.templates.etl.common;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

/**
 * Class that extends {@link DBInputFormat} to load the database driver class correctly.
 */
public class ETLDBInputFormat extends DBInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(ETLDBInputFormat.class);

  @Override
  public Connection getConnection() {
    Configuration conf = getConf();
    ClassLoader classLoader = conf.getClassLoader();
    if (this.connection == null) {
      try {
        LOG.debug("Registering JDBC driver via shim - " + JDBCDriverShim.class.getName());
        Class<?> driverClass = classLoader.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
        String url = conf.get(DBConfiguration.URL_PROPERTY);
        DriverManager.registerDriver(new JDBCDriverShim((Driver) driverClass.newInstance()));
        if (conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
          this.connection = DriverManager.getConnection(url);
        } else {
          this.connection = DriverManager.getConnection(url,
                                                        conf.get(DBConfiguration.USERNAME_PROPERTY),
                                                        conf.get(DBConfiguration.PASSWORD_PROPERTY));
        }
        this.connection.setAutoCommit(false);
        this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return this.connection;
  }
}
