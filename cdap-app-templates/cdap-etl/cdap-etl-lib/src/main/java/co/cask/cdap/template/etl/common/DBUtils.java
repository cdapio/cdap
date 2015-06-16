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

import co.cask.cdap.template.etl.batch.sink.DBSink;
import co.cask.cdap.template.etl.batch.source.DBSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Utility methods for MySQL shared by {@link DBSource} and {@link DBSink}
 */
public final class DBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);

  /**
   * Performs any Database related cleanup
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  public static void cleanup(ClassLoader classLoader) {
    shutDownMySQLAbandonedConnectionCleanupThread(classLoader);
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy
   * If this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    try {
      if (classLoader == null) {
        return;
      }
      Class<?> mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      if (mysqlCleanupThreadClass == null) {
        // Ignore. Could perhaps be a different database other than MySQL.
        return;
      }
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      if (shutdownMethod != null) {
        shutdownMethod.invoke(null);
        LOG.info("Successfully shutdown MySQL connection cleanup thread.");
      }
    } catch (Throwable e) {
      // cleanup failed, ignoring silently
      LOG.info("Failed to shutdown MySQL connection cleanup thread. Ignoring.", e);
    }
  }

  private DBUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
