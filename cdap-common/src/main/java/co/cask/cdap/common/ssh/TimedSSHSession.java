/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.runtime.spi.ssh.PortForwarding;
import co.cask.cdap.runtime.spi.ssh.SSHProcess;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 *
 */
public class TimedSSHSession implements SSHSession {
  private static final Logger LOG = LoggerFactory.getLogger(TimedSSHSession.class);
  private static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  private final DefaultSSHSession defaultSSHSession;
  private final long timeoutMillis;
  private final ExecutorService executor;

  public TimedSSHSession(DefaultSSHSession defaultSSHSession) {
    this.defaultSSHSession = defaultSSHSession;
    this.timeoutMillis = DEFAULT_TIMEOUT;
    this.executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("timed-ssh-exec-%d"));
  }

  @Override
  public boolean isAlive() {
    return defaultSSHSession.isAlive();
  }

  @Override
  public InetSocketAddress getAddress() {
    return defaultSSHSession.getAddress();
  }

  @Override
  public String getUsername() {
    return defaultSSHSession.getUsername();
  }

  @Override
  public SSHProcess execute(List<String> commands) throws IOException {
    return defaultSSHSession.execute(commands);
  }

  @Override
  public String executeAndWait(List<String> commands) throws IOException {
    return defaultSSHSession.executeAndWait(commands);
  }

  @Override
  public void copy(Path sourceFile, String targetPath) throws IOException {
    // No need to time since this just calls the other copy method
    defaultSSHSession.copy(sourceFile, targetPath);
  }

  @Override
  public void copy(InputStream input, String targetPath, String targetName, long size, int permission,
                   @Nullable Long lastAccessTime, @Nullable Long lastModifiedTime) throws IOException {
    timedCopy(() -> {
      defaultSSHSession.copy(input, targetPath, targetName, size, permission, lastAccessTime, lastModifiedTime);
      return null;
    });
  }

  @Override
  public PortForwarding createLocalPortForward(String targetHost, int targetPort, int originatePort,
                                               PortForwarding.DataConsumer dataConsumer) throws IOException {
    return defaultSSHSession.createLocalPortForward(targetHost, targetPort, originatePort, dataConsumer);
  }

  @Override
  public void close() {
    defaultSSHSession.close();
    executor.shutdown();
  }

  @Override
  public SSHProcess execute(String... commands) throws IOException {
    return defaultSSHSession.execute(commands);
  }

  @Override
  public String executeAndWait(String... commands) throws IOException {
    return defaultSSHSession.executeAndWait(commands);
  }

  private void timedCopy(Callable<Void> callable) throws IOException {
    // TODO: change log level to debug
    LOG.info("Running copy with timeout {} ms", timeoutMillis);
    Future<?> future = null;
    try {
      future = executor.submit(callable);
      future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    } catch (TimeoutException e) {
      future.cancel(true);
      throw new IOException(String.format("scp took longer than %d ms:", timeoutMillis));
    }
  }
}
