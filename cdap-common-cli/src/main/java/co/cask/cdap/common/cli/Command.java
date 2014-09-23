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

package co.cask.cdap.common.cli;

import java.io.PrintStream;

/**
 * An executable command that takes in arguments.
 */
public interface Command {

  /**
   * Executes this command, given some arguments.
   *
   * @param arguments the argument values from the input pattern ({@link #getPattern()}.
   *                  e.g. with {@code <blog-name>} in the input pattern, the value from the input
   *                  can be retrieved via {@code arguments.get("blog-name")}.
   * @param output {@link java.io.PrintStream} to write output to
   * @throws Exception if something went wrong
   */
  public void execute(Arguments arguments, PrintStream output) throws Exception;

  /**
   * @return input pattern that activates this {@link Command}.
   * May contain required and optional arguments. For example,
   * an input of "create blog sdf" would activate this command if
   * the pattern were "create blog <blog-name>".
   */
  public String getPattern();

  /**
   * @return short descriptive text describing what this {@link Command} does
   */
  public String getDescription();

}
