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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.templates.plugins.PluginConfig;

import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that both the RDBMS source and sink can re-use
 */
public class DBConfig extends PluginConfig {

  @Description("JDBC connection string including database name.")
  public String connectionString;

  @Description("User identity for connecting to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String user;

  @Description("Password to use to connect to the specified database. Required for databases that " +
    "need authentication. Optional for databases that do not require authentication.")
  @Nullable
  public String password;

  @Description("Name of the JDBC plugin to use. This is the value of the 'name' key defined in the JSON file " +
    "for the JDBC plugin.")
  public String jdbcPluginName;

  @Description("Type of the JDBC plugin to use. This is the value of the 'type' key defined in the JSON file " +
    "for the JDBC plugin. Defaults to 'jdbc'.")
  @Nullable
  public String jdbcPluginType = "jdbc";

  public DBConfig() {
    jdbcPluginType = "jdbc";
  }
}
