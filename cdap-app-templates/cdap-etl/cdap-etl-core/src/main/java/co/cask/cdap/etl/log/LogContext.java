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

package co.cask.cdap.etl.log;

import com.google.common.base.Throwables;
import org.slf4j.MDC;

import java.util.concurrent.Callable;

/**
 * Controls what the current ETL stage is for logging. Log messages will be prefixed with the ETL
 * stage name if it is set.
 */
public class LogContext {
  private static volatile boolean enabled = false;
  static final String STAGE = "cdap.etl.stage";

  private LogContext() {

  }

  public static void enable() {
    enabled = true;
  }

  /**
   * Run the specified command with the specified stage name. Messages logged within the command
   * will be prefixed with the stage name unless they are logged within a call to {@link #runWithoutLogging(Callable)}.
   *
   * @param command the command to run
   * @param stageName the name of the stage the command is being run in
   * @param <T> the type of object returned by the command
   * @return the result of running the command
   * @throws Exception if the command throws an exception
   */
  public static <T> T run(Callable<T> command, String stageName) throws Exception {
    if (enabled) {
      MDC.put(STAGE, stageName);
      try {
        return command.call();
      } finally {
        MDC.remove(STAGE);
      }
    } else {
      return command.call();
    }
  }

  /**
   * Run the specified command with the specified stage name. Messages logged within the command
   * will be prefixed with the stage name unless they are logged within a call to {@link #runWithoutLogging(Callable)}.
   * This can be called instead of {@link #run(Callable, String)} if the command does not throw any checked
   * exceptions.
   *
   * @param command the command to run
   * @param stageName the name of the stage the command is being run in
   * @param <T> the type of object returned by the command
   * @return the result of running the command
   */
  public static <T> T runUnchecked(Callable<T> command, String stageName) {
    if (enabled) {
      MDC.put(STAGE, stageName);
      try {
        return runUnchecked(command);
      } finally {
        MDC.remove(STAGE);
      }
    } else {
      return runUnchecked(command);
    }
  }

  /**
   * Run the specified command and leave out the stage name prefix for messages logged within the command.
   * Used to run CDAP system calls within a command passed to {@link #run(Callable, String)}.
   *
   * @param command the command to run
   * @param <T> the type of object returned by the command
   * @return the result of running the command
   * @throws Exception if the command throws an exception
   */
  public static <T> T runWithoutLogging(Callable<T> command) throws Exception {
    if (enabled) {
      String stage = MDC.get(STAGE);
      if (stage == null) {
        return command.call();
      }

      MDC.remove(STAGE);
      try {
        return command.call();
      } finally {
        MDC.put(STAGE, stage);
      }
    } else {
      return command.call();
    }
  }

  /**
   * Run the specified command and leave out the stage name prefix for messages logged within the command.
   * Used to run CDAP system calls within a command passed to {@link #run(Callable, String)}.
   * This can be called instead of {@link #runWithoutLogging(Callable)} if the command does not throw any checked
   * exceptions.
   *
   * @param command the command to run
   * @param <T> the type of object returned by the command
   * @return the result of running the command
   */
  public static <T> T runWithoutLoggingUnchecked(Callable<T> command) {
    if (enabled) {
      String stage = MDC.get(STAGE);

      if (stage == null) {
        return runUnchecked(command);
      }

      MDC.remove(STAGE);
      try {
        return runUnchecked(command);
      } finally {
        MDC.put(STAGE, stage);
      }
    } else {
      return runUnchecked(command);
    }
  }

  private static <T> T runUnchecked(Callable<T> command) {
    try {
      return command.call();
    } catch (Exception e) {
      Throwables.propagateIfPossible(e);
      // this shouldn't happen unless the caller is behaving badly and passes
      // in a command that does throw a check exception
      throw Throwables.propagate(e);
    }
  }
}
