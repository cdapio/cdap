/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.runtime.spi.ssh;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents a process launched via the {@link SSHSession#execute(String...)} method.
 */
public interface SSHProcess {

  /**
   * Returns an {@link OutputStream} for writing to the remote process stdin.
   *
   * @return an {@link OutputStream}
   * @throws IOException if failed to open the stream
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * Returns an {@link InputStream} for reading from the remote process stdout.
   *
   * @return an {@link InputStream}
   * @throws IOException if failed to open the stream
   */
  InputStream getInputStream() throws IOException;

  /**
   * Returns an {@link InputStream} for reading from the remote process stderr.
   *
   * @return an {@link InputStream}
   * @throws IOException if failed to open the stream
   */
  InputStream getErrorStream() throws IOException;

  /**
   * Blocks for the remote process to finish.
   *
   * @return the exit code of the process
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  int waitFor() throws InterruptedException;

  /**
   * Blocks for the remote process to finish.
   *
   * @param timeout the maximum time to wait
   * @param unit    the {@link TimeUnit} for the timeout
   * @return the exit code of the process
   * @throws TimeoutException     if the process is not yet terminated after the given timeout
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  int waitFor(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException;

  /**
   * Returns the exit code of the remote process if it was completed.
   *
   * @throws IllegalThreadStateException if the process is not yet terminated
   */
  int exitValue() throws IllegalThreadStateException;

  /**
   * Attempts to stop the remote process by closing ssh channel.
   */
  void destroy();
}
