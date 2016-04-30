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

import co.cask.cdap.api.common.Bytes;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.PrintStream;
import javax.annotation.Nullable;

/**
 * Redirects print stream
 */
public final class RedirectedPrintStream extends FilterOutputStream {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final ByteArrayOutputStream byteArrayOutputStream;
  private final PrintStream outStream;
  private final boolean errorStream;
  private final Logger logger;

  public static RedirectedPrintStream createRedirectedOutStream(Logger logger, @Nullable PrintStream outStream) {
    return new RedirectedPrintStream(logger, outStream, false);
  }

  public static RedirectedPrintStream createRedirectedErrStream(Logger logger, @Nullable PrintStream outStream) {
    return new RedirectedPrintStream(logger, outStream, true);
  }

  private RedirectedPrintStream(Logger logger, @Nullable PrintStream outStream, boolean errorStream) {
    super(new ByteArrayOutputStream());
    // Safe cast as we know what outputStream we've created.
    byteArrayOutputStream = (ByteArrayOutputStream) super.out;
    this.outStream = outStream;
    this.errorStream = errorStream;
    this.logger = logger;
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

    if (errorStream) {
      logger.error(msg);
    } else {
      logger.info(msg);
    }

    byteArrayOutputStream.reset();
    if (outStream != null) {
      outStream.flush();
    }
  }

  @Override
  public void write(int i) {
    byteArrayOutputStream.write(i);
    if (outStream != null) {
      outStream.write(i);
    }
  }
}
