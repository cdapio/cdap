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

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;

/**
 * Class that extends {@link DBInputFormat} to load the database driver class correctly.
 */
public class ETLDBInputFormat extends DBInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(ETLDBInputFormat.class);
  private static Driver driver;
  private static JDBCDriverShim driverShim;

  @Override
  public Connection getConnection() {
    if (this.connection == null) {
      Configuration conf = getConf();
      try {
        String url = conf.get(DBConfiguration.URL_PROPERTY);
        try {
          // throws SQLException if no suitable driver is found
          DriverManager.getDriver(url);
        } catch (SQLException e) {
          if (driver == null) {
            ClassLoader classLoader = conf.getClassLoader();
            Class<?> driverClass = classLoader.loadClass(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));
            driver = (Driver) driverClass.newInstance();

            Field field = DriverManager.class.getDeclaredField("registeredDrivers");
            field.setAccessible(true);
            CopyOnWriteArrayList<?> list = (CopyOnWriteArrayList<?>) field.get(null);
            for (Object driverInfo : list) {
              Class<?> driverInfoClass = classLoader.loadClass("java.sql.DriverInfo");
              Field driverField = driverInfoClass.getDeclaredField("driver");
              driverField.setAccessible(true);
              Driver d = (Driver) driverField.get(driverInfo);
              ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
              if (registeredDriverClassLoader == null) {
                LOG.info("Found null classloader for driver {}. Ignoring since this may be using system classloader.",
                         d.getClass().getName());
                continue;
              }
              if (registeredDriverClassLoader.equals(driver.getClass().getClassLoader())) {
                LOG.info("Removing driver {} from registeredDrivers", d.getClass().getName());
                list.remove(driverInfo);
              }
            }

            driverShim = new JDBCDriverShim(driver);
            DriverManager.registerDriver(driverShim);
            LOG.info("Registered JDBC driver via shim {}, hashcode  {}, class {}", driverShim, driverShim.hashCode(),
                     driverShim.getClass().getName());
            LOG.info("Actual driver {} {} {}", driver, driver.hashCode(), driver.getClass().getName());
          }
        }
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

  public static void deregisterDrivers() {
    deregisterDriver(driverShim);
    deregisterDriver(driver);
  }

  private static void deregisterDriver(@Nullable Driver driver) {
    if (driver == null) {
      return;
    }
    try {
      DriverManager.deregisterDriver(driver);
      LOG.info("Successfully deregistered driver {} {} {}", driver, driver.hashCode(), driver.getClass().getName());
    } catch (Throwable e) {
      LOG.warn("Error while deregistering driver {}", driver.getClass().getName());
      throw Throwables.propagate(e);
    }
  }
}
