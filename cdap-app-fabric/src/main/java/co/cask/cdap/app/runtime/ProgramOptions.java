/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime;

/**
 * Represents options for a program execution.
 */
public interface ProgramOptions {

  /**
   * Returns the name of the program.
   */
  String getName();

  /**
   * Returns the system arguments. It is for storing arguments used by the runtime system.
   */
  Arguments getArguments();

  /**
   * Returns the user arguments.
   */
  Arguments getUserArguments();

  /**
   * Returns {@code true} if executing in debug mode.
   */
  boolean isDebug();
}
