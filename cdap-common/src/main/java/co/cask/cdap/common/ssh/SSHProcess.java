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

package co.cask.cdap.common.ssh;

import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import com.jcraft.jsch.ChannelExec;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents a process launched via the {@link SSHSession#execute(String...)} method.
 */
public final class SSHProcess {

  private final ChannelExec channelExec;

  SSHProcess(ChannelExec channelExec) {
    this.channelExec = channelExec;
  }

  /**
   * Returns an {@link OutputStream} for writing to the remote process stdin.
   *
   * @return an {@link OutputStream}
   * @throws IOException if failed to open the stream
   */
  public OutputStream getOutputStream() throws IOException {
    return channelExec.getOutputStream();
  }

  /**
   * Returns an {@link InputStream} for reading from the remote process stdout.
   *
   * @return an {@link InputStream}
   * @throws IOException if failed to open the stream
   */
  public InputStream getInputStream() throws IOException {
    return new ChannelInputStream(channelExec.getInputStream());
  }

  /**
   * Returns an {@link InputStream} for reading from the remote process stderr.
   *
   * @return an {@link InputStream}
   * @throws IOException if failed to open the stream
   */
  public InputStream getErrorStream() throws IOException {
    return new ChannelInputStream(channelExec.getErrStream());
  }

  /**
   * Blocks for the remote process to finish.
   *
   * @param timeout the maximum time to wait
   * @param unit the {@link TimeUnit} for the timeout
   * @return the exit code of the process
   * @throws TimeoutException if the process is not yet terminated after the given timeout
   * @throws InterruptedException if this thread is interrupted while waiting
   */
  public int waitFor(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
    RetryStrategy retry = RetryStrategies.timeLimit(timeout, unit,
                                                    RetryStrategies.fixDelay(100, TimeUnit.MILLISECONDS));
    try {
      return Retries.supplyWithRetries(this::exitValue, retry, IllegalThreadStateException.class::isInstance);
    } catch (IllegalThreadStateException e) {
      throw new TimeoutException("Process is still running");
    }
  }

  /**
   * Returns the exit code of the remote process if it was completed.
   *
   * @throws IllegalThreadStateException if the process is not yet terminated
   */
  public int exitValue() throws IllegalThreadStateException {
    int exitStatus = channelExec.getExitStatus();
    if (exitStatus == -1) {
      if (!channelExec.isConnected()) {
        // exit status for SIGHUP
        return 129;
      }
      throw new IllegalThreadStateException("Process not terminated");
    }
    return exitStatus;
  }

  /**
   * Attempts to stop the remote process by closing ssh channel.
   */
  public void destroy() {
    channelExec.disconnect();
  }

  /**
   * Wrapper for {@link InputStream} coming from {@link ChannelExec}. It checks if the {@link ChannelExec} is
   * closed and return EOF if it does.
   */
  private final class ChannelInputStream extends FilterInputStream {

    ChannelInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read() throws IOException {
      if (in.available() == 0 && channelExec.isClosed()) {
        return -1;
      }
      return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (in.available() == 0 && channelExec.isClosed()) {
        return -1;
      }
      return super.read(b, off, len);
    }
  }
}
