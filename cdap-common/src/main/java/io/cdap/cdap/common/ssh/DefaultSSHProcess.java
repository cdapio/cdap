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
import co.cask.cdap.runtime.spi.ssh.SSHProcess;
import com.jcraft.jsch.ChannelExec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of {@link SSHProcess} used by {@link DefaultSSHSession}.
 */
final class DefaultSSHProcess implements SSHProcess {

  private final ChannelExec channelExec;
  private final OutputStream outputStream;
  private final InputStream inputStream;
  private final InputStream errorStream;

  DefaultSSHProcess(ChannelExec channelExec, OutputStream outputStream,
                    InputStream inputStream, InputStream errorStream) {
    this.channelExec = channelExec;
    this.outputStream = outputStream;
    this.inputStream = inputStream;
    this.errorStream = errorStream;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return outputStream;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return inputStream;
  }

  @Override
  public InputStream getErrorStream() throws IOException {
    return errorStream;
  }

  @Override
  public int waitFor() throws InterruptedException {
    RetryStrategy retry = RetryStrategies.fixDelay(100, TimeUnit.MILLISECONDS);
    return Retries.supplyWithRetries(this::exitValue, retry, IllegalThreadStateException.class::isInstance);
  }

  @Override
  public int waitFor(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
    RetryStrategy retry = RetryStrategies.timeLimit(timeout, unit,
                                                    RetryStrategies.fixDelay(100, TimeUnit.MILLISECONDS));
    try {
      return Retries.supplyWithRetries(this::exitValue, retry, IllegalThreadStateException.class::isInstance);
    } catch (IllegalThreadStateException e) {
      throw new TimeoutException("Process is still running");
    }
  }

  @Override
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

  @Override
  public void destroy() {
    channelExec.disconnect();
  }
}
