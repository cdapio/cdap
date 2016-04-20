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

package co.cask.cdap.explore.service;

import co.cask.cdap.common.logging.RedirectedPrintStream;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * Sets the output streams of a {@link SessionState} to a logger instead of Hive's default System.out and System.err
 */
public final class HiveStreamRedirector {

  public static void redirectToLogger(SessionState sessionState) {
    redirectToLogger(sessionState, null);
  }

  @VisibleForTesting
  static void redirectToLogger(SessionState sessionState, Logger logger) {
    Logger logOut = (logger == null) ? LoggerFactory.getLogger("Explore.stdout") : logger;
    Logger logErr = (logger == null) ? LoggerFactory.getLogger("Explore.stderr") : logger;

    sessionState.err = new PrintStream(RedirectedPrintStream.createRedirectedErrStream(logErr, null), true);
    sessionState.out = new PrintStream(RedirectedPrintStream.createRedirectedOutStream(logOut, null), true);

    sessionState.childErr = new PrintStream(RedirectedPrintStream.createRedirectedErrStream(logErr, null), true);
    sessionState.childOut = new PrintStream(RedirectedPrintStream.createRedirectedOutStream(logOut, null), true);
  }

  private HiveStreamRedirector() {
  }
}
