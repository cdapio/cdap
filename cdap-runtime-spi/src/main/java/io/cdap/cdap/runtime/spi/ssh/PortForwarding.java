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

package io.cdap.cdap.runtime.spi.ssh;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Represents a SSH port forwarding channel.
 */
public interface PortForwarding extends WritableByteChannel, Flushable {

  @Override
  int write(ByteBuffer buf) throws IOException;

  @Override
  void flush() throws IOException;

  @Override
  boolean isOpen();

  @Override
  void close() throws IOException;

  /**
   * This class is for consuming incoming data from the SSH port forwarding channel.
   */
  abstract class DataConsumer {

    /**
     * Invoked when new data is received.
     *
     * @param buffer the content of the data received
     */
    public abstract void received(ByteBuffer buffer);

    /**
     * Invoked when there is a flush event received from the channel.
     */
    public void flushed() {
      // no-op
    }

    /**
     * Invoked when the channel is closed and no more data will be received.
     */
    public void finished() {
      // no-op
    }
  }
}
