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
 * Redirected {@link PrintStream} to logger
 */
public final class RedirectedPrintStream extends FilterOutputStream {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final ByteArrayOutputStream byteArrayOutputStream;
  private final PrintStream outStream;
  private final Logger logger;
  private final boolean isErrorStream;

  /**
   * Creates {@link RedirectedPrintStream} from given outstream which will be redirected to given logger.
   * @param logger logger object to which outstream will be redirected to.
   * @param outStream output stream which will be redirected to logger object.
   * @return The instance of a {@link RedirectedPrintStream}.
   */
  public static RedirectedPrintStream createRedirectedOutStream(Logger logger, @Nullable PrintStream outStream) {
    return new RedirectedPrintStream(logger, outStream, false);
  }

  /**
   * Creates {@link RedirectedPrintStream} from given errorstream which will be redirected to given logger.
   * @param logger logger object to which outstream will be redirected to.
   * @param errorStream error stream which will be redirected to logger object.
   * @return The instance of a {@link RedirectedPrintStream}.
   */
  public static RedirectedPrintStream createRedirectedErrStream(Logger logger, @Nullable PrintStream errorStream) {
    return new RedirectedPrintStream(logger, errorStream, true);
  }

  private RedirectedPrintStream(Logger logger, @Nullable PrintStream outStream, boolean isErrorStream) {
    super(new ByteArrayOutputStream());
    // Safe cast as we know what outputStream we've created.
    byteArrayOutputStream = (ByteArrayOutputStream) super.out;
    this.logger = logger;
    this.outStream = outStream;
    this.isErrorStream = isErrorStream;
  }

  @Override
  public void flush() {
    if (outStream != null) {
      outStream.flush();
    }

    String msg = Bytes.toString(byteArrayOutputStream.toByteArray());
    byteArrayOutputStream.reset();

    if (msg == null || msg.isEmpty()) {
      return;
    }

    if (msg.endsWith(LINE_SEPARATOR)) {
      msg = msg.substring(0, msg.length() - LINE_SEPARATOR.length());
    }

    if (msg.isEmpty()) {
      return;
    }

    // change level of logging for error stream
    if (isErrorStream) {
      logger.warn(msg);
    } else {
      logger.info(msg);
    }
  }

  @Override
  public void write(int i) {
    byteArrayOutputStream.write(i);
    if (outStream != null) {
      outStream.write(i);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) {
    byteArrayOutputStream.write(b, off, len);
    if (outStream != null) {
      outStream.write(b, off, len);
    }
  }
}
