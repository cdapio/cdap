/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Redirects standard out and standard error to logger
 */
@SuppressWarnings("unused")
public final class StandardOutErrorRedirector {
  /**
   * Redirect standard out and error to logger
   * @param loggerName Name of the logger to which stdout
   */
  public static void redirectToLogger(String loggerName) {
    Logger logger = LoggerFactory.getLogger(loggerName);
    System.setOut(new PrintStream(RedirectedPrintStream.createRedirectedOutStream(logger, System.out), true));
    System.setErr(new PrintStream(RedirectedPrintStream.createRedirectedErrStream(logger, System.err), true));
  }
}
