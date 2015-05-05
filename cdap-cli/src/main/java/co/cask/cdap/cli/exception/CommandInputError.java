/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.exception;

import co.cask.common.cli.Command;

/**
 * Thrown when there was an error in the command input.
 */
public class CommandInputError extends RuntimeException {

  public CommandInputError(Command command) {
    super("Invalid input. Expected format: " + command.getPattern());
  }

  public CommandInputError(Command command, String message) {
    super("Invalid input: " + message + "\nExpected format: " + command.getPattern());
  }
}
