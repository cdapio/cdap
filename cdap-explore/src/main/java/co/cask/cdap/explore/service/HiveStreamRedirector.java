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

import co.cask.cdap.api.common.Bytes;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.PrintStream;
import javax.annotation.Nullable;

/**
 * Sets the output streams of a {@link SessionState} to a logger instead of Hive's default System.out and System.err
 */
public final class HiveStreamRedirector {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  public static void redirectToLogger(SessionState sessionState) {
    redirectToLogger(sessionState, null);
  }

  @VisibleForTesting
  static void redirectToLogger(SessionState sessionState, Logger logger) {
    sessionState.err = new PrintStream(new RedirectedPrintStream(true, logger), true);
    sessionState.out = new PrintStream(new RedirectedPrintStream(false, logger), true);

    sessionState.childErr = new PrintStream(new RedirectedPrintStream(true, logger), true);
    sessionState.childOut = new PrintStream(new RedirectedPrintStream(false, logger), true);
  }

  private static final class RedirectedPrintStream extends FilterOutputStream {
    private static final Logger LOG_OUT = LoggerFactory.getLogger("Explore.stdout");
    private static final Logger LOG_ERR = LoggerFactory.getLogger("Explore.stderr");

    private final ByteArrayOutputStream byteArrayOutputStream;
    private final boolean errorStream;
    private final Logger logger;

    RedirectedPrintStream(boolean errorStream, @Nullable Logger logger) {
      super(new ByteArrayOutputStream());
      // Safe cast as we know what outputStream we've created.
      byteArrayOutputStream = (ByteArrayOutputStream) super.out;
      this.errorStream = errorStream;
      this.logger = logger == null ? getLogger() : logger;
    }

    private Logger getLogger() {
      return errorStream ? LOG_ERR : LOG_OUT;
    }

    @Override
    public void flush() {
      String msg = Bytes.toString(byteArrayOutputStream.toByteArray());
      if (msg == null || msg.isEmpty()) {
        byteArrayOutputStream.reset();
        return;
      }

      if (msg.endsWith(LINE_SEPARATOR)) {
        msg = msg.substring(0, msg.length() - LINE_SEPARATOR.length());
      }

      if (msg.isEmpty()) {
        byteArrayOutputStream.reset();
        return;
      }

      logger.info(msg);

      byteArrayOutputStream.reset();
    }

    @Override
    public void write(int i) {
      byteArrayOutputStream.write(i);
    }
  }

  private HiveStreamRedirector() {
  }
}
