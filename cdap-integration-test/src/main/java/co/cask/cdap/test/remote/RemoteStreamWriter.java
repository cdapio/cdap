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

package co.cask.cdap.test.remote;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class RemoteStreamWriter implements StreamWriter {

  private final StreamClient streamClient;
  private final String streamName;

  public RemoteStreamWriter(ClientConfig clientConfig, String streamName) {
    this.streamClient = new StreamClient(clientConfig);
    this.streamName = streamName;
  }

  @Override
  public void send(String content) throws IOException {
    try {
      streamClient.sendEvent(streamName, content);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(byte[] content) throws IOException {
    try {
      streamClient.sendEvent(streamName, new String(content, Charsets.UTF_8));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    try {
      streamClient.sendEvent(streamName, new String(content, off, len, Charsets.UTF_8));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    try {
      streamClient.sendEvent(streamName, Bytes.toString(buffer));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(Map<String, String> headers, String content) throws IOException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void send(Map<String, String> headers, byte[] content) throws IOException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void send(Map<String, String> headers, ByteBuffer buffer) throws IOException {
    throw new UnsupportedOperationException("TODO");
  }
}
