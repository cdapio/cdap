/*
 * Copyright © 2021 Cask Data, Inc.
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

/**
 * Class used to represent exceptions thrown by a SQL Engine.
 */
public class SQLEngineException extends RuntimeException {

  public SQLEngineException(String message) {
    super(message);
  }

  public SQLEngineException(Throwable cause) {
    super("Error when executing operation on SQL Engine", cause);
  }

  public SQLEngineException(String message, Throwable cause) {
    super(message, cause);
  }
}
