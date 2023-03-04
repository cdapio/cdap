/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql;

import io.cdap.cdap.api.data.batch.Input;
import java.util.Map;

/**
 * Sources that want to employ optimization by an SQL Engine may push SQLEngine-specific inputs,
 * which include a fallback input in case the SQL Engine Input cannot be processed.
 */
public class SQLEngineInput extends Input {

  private final String stageName;
  private final String sqlEngineClassName;
  private final Map<String, String> arguments;

  public SQLEngineInput(String name,
      String stageName,
      String sqlEngineClassName,
      Map<String, String> arguments) {
    super(name);
    this.stageName = stageName;
    this.sqlEngineClassName = sqlEngineClassName;
    this.arguments = arguments;
  }

  /**
   * Gets the stage name for this input. This name is used to allocate metrics to the appropriate
   * sink after the input is written into the SQL engine
   *
   * @return the stage name
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * Gets the class name for the SQL engine implementation
   *
   * @return class name for the SQL engine implementation
   */
  public String getSqlEngineClassName() {
    return sqlEngineClassName;
  }

  /**
   * Get arguments used for input configuration
   *
   * @return arguments used for input configuration
   */
  public Map<String, String> getArguments() {
    return arguments;
  }

  @Override
  public String toString() {
    return "SQLEngineInput{"
        + "name='" + getName() + '\''
        + ", sqlEngineClassName='" + sqlEngineClassName + '\''
        + "} ";
  }
}
