/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.utils;

/**
 * This exception is only for bailing out of a command line tool when
 * the command line arguments are not valid. In that case, we want to
 * print a usage statement, but not an exception, and we also don't want
 * to call System.exit(). So, when the program detects an error, it prints
 * the help text and throws this exception. At the top level, we can catch
 * this and exit without printing. We could use IllegalArgumentException
 * instead, but we never know whether that was thrown somewhere else,
 * in which case we want to print the exception.
 */
public class UsageException extends RuntimeException {
  // no message, no cause, on purpose, only default constructor
  public UsageException() { }
}
