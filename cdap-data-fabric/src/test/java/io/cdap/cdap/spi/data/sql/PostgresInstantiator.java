/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.sql;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.io.File;
import java.io.IOException;

/**
 * Used to instantiate {@link EmbeddedPostgres}.
 */
public class PostgresInstantiator {

  private PostgresInstantiator() {}

  /**
   * Create and start an embedded postgres instance. The instance has to be closed after usage.
   *
   * @param tempFolder The temp folder to create the postgres directories
   * @return Embedded postgres instance that is started
   * @throws IOException on errors during creation or start
   */
  public static EmbeddedPostgres createAndStart(File tempFolder) throws IOException {
    return createAndStart(CConfiguration.create(), tempFolder);
  }

  /**
   * Create and start an embedded postgres instance. The instance has to be closed after usage.
   *
   * @param cConf Adds sql specific configuration values to cConf
   * @param tempFolder The temp folder to create the postgres directories
   * @return Embedded postgres instance that is started
   * @throws IOException on errors during creation or start
   */
  public static EmbeddedPostgres createAndStart(CConfiguration cConf, File tempFolder) throws IOException {
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);
    cConf.setBoolean(Constants.Dataset.DATA_STORAGE_SQL_DRIVER_EXTERNAL, false);

    EmbeddedPostgres pg = EmbeddedPostgres.builder()
      .setDataDirectory(new File(tempFolder, "data"))
      .setCleanDataDirectory(false)
      .setOverrideWorkingDirectory(new File(tempFolder, "pg"))
      .start();
    String jdbcUrl = pg.getJdbcUrl("postgres", "postgres");
    cConf.set(Constants.Dataset.DATA_STORAGE_SQL_JDBC_CONNECTION_URL, jdbcUrl);
    return pg;
  }
}
