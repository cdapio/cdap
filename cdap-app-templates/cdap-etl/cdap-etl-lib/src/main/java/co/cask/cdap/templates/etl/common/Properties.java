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

/**
 * Class to define property names for sources and sinks
 */
public final class Properties {

  /**
   * Class to hold properties for DBSource and DBSink
   */
  public static class DB {
    public static final String DRIVER_CLASS = "dbDriverClass";
    public static final String CONNECTION_STRING = "dbConnectionString";
    public static final String TABLE_NAME = "dbTableName";
    public static final String USER = "dbUser";
    public static final String PASSWORD = "dbPassword";
    public static final String COLUMNS = "dbColumns";
    public static final String IMPORT_QUERY = "dbImportQuery";
    public static final String COUNT_QUERY = "dbCountQuery";
  }

  private Properties() {
  }
}
