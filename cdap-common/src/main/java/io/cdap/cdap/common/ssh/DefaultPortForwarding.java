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

package io.cdap.cdap.common.ssh;

import com.jcraft.jsch.ChannelDirectTCPIP;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of {@link PortForwarding}.
 */
final class DefaultPortForwarding implements PortForwarding {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultPortForwarding.class);

  private static final int TRANSFER_SIZE = 8192;

  private final ChannelDirectTCPIP sshChannel;
  private final OutputStream outputStream;
  private final byte[] transferBuf;

  /**
   * Creates a new instance of this class by connecting the direct TCPIP channel for port forwarding.
   *
   * @param sshChannel an unconnected {@link ChannelDirectTCPIP} created from a ssh session
   * @param dataConsumer the {@link DataConsumer} for receiving incoming data
   * @throws IOException if failed to connect to the channel
   */
  DefaultPortForwarding(ChannelDirectTCPIP sshChannel, DataConsumer dataConsumer) throws IOException {
    sshChannel.setOutputStream(createIncomingOutputStream(dataConsumer));

    this.outputStream = sshChannel.getOutputStream();
    this.sshChannel = sshChannel;
    this.transferBuf = new byte[TRANSFER_SIZE];

    try {
      sshChannel.connect();
      Session session = sshChannel.getSession();
      LOG.trace("Opened port forwarding channel {} through host {}:{}",
                sshChannel.getId(), session.getHost(), session.getPort());
    } catch (JSchException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int write(ByteBuffer buf) throws IOException {
    if (!isOpen()) {
      throw new IOException("Port forwarding channel is not opened");
    }

    int remaining = buf.remaining();
    writeFully(buf);

    return remaining;
  }

  @Override
  public synchronized void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public boolean isOpen() {
    return sshChannel.isConnected();
  }

  @Override
  public synchronized void close() {
    Session session = null;
    try {
      session = sshChannel.getSession();
    } catch (JSchException e) {
      // Ignore as it only affects logging and normally shouldn't happen
    }

    int id = sshChannel.getId();
    sshChannel.disconnect();

    if (session != null) {
      LOG.trace("Disconnected port forwarding channel {} through host {}:{}", id, session.getHost(), session.getPort());
    } else {
      LOG.trace("Disconnected port forwarding channel {}", id);
    }
  }

  /**
   * Writes all the bytes from the given {@link ByteBuffer} to the channel {@link OutputStream}.
   *
   * @param buf the {@link ByteBuffer} to write
   * @throws IOException if an I/O error occurs
   */
  private synchronized void writeFully(ByteBuffer buf) throws IOException {
    // If the ByteBuffer is backed by array, use it directly without copying
    if (buf.hasArray()) {
      outputStream.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
      buf.position(buf.limit());
      return;
    }

    // Otherwise, we have to copy the content into a byte[] and write to the OutputStream
    int remaining = buf.remaining();
    while (remaining > 0) {
      int len = Math.min(remaining, transferBuf.length);
      buf.get(transferBuf, 0, len);
      outputStream.write(transferBuf, 0, len);
      remaining = buf.remaining();
    }
  }

  /**
   * Creates the {@link OutputStream} that will be provided to the SSH channel for receiving incoming data.
   *
   * @param dataConsumer the {@link DataConsumer} to invoke when there is data received.
   * @return an {@link OutputStream}
   */
  private OutputStream createIncomingOutputStream(DataConsumer dataConsumer) {
    byte[] oneByte = new byte[1];

    final AtomicBoolean closed = new AtomicBoolean();
    return new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        oneByte[0] = (byte) b;
        write(oneByte, 0, 1);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        if (closed.get()) {
          throw new IOException("Channel already closed");
        }
        dataConsumer.received(ByteBuffer.wrap(b, off, len));
      }

      @Override
      public void flush() throws IOException {
        if (closed.get()) {
          throw new IOException("Channel already closed");
        }

        dataConsumer.flushed();
      }

      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          dataConsumer.finished();
        }
      }
    };
  }
}
