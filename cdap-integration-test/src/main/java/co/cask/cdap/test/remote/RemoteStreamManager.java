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
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.StreamManager;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link StreamManager} that interacts with stream service via HTTP calls
 */
public class RemoteStreamManager implements StreamManager {
  private final StreamClient streamClient;
  private final StreamId streamId;

  public RemoteStreamManager(ClientConfig clientConfig, RESTClient restClient, StreamId streamId) {
    this.streamClient = new StreamClient(clientConfig, restClient);
    this.streamId = streamId;
  }

  @Override
  public void createStream() throws IOException {
    try {
      streamClient.create(streamId);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(String content) throws IOException {
    try {
      streamClient.sendEvent(streamId, content);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(byte[] content) throws IOException {
    try {
      streamClient.sendEvent(streamId, new String(content, Charsets.UTF_8));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    try {
      streamClient.sendEvent(streamId, new String(content, off, len, Charsets.UTF_8));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    try {
      streamClient.sendEvent(streamId, Bytes.toString(buffer));
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

  @Override
  public void send(File file, String contentType) throws Exception {
    streamClient.sendFile(streamId, contentType, file);
  }

  @Override
  public List<StreamEvent> getEvents(long startTime, long endTime, int limit) throws IOException {
    return getEvents(String.valueOf(startTime), String.valueOf(endTime), limit);
  }

  @Override
  public List<StreamEvent> getEvents(String startTime, String endTime, int limit) throws IOException {
    List<StreamEvent> results = Lists.newArrayList();
    try {
      streamClient.getEvents(streamId, startTime, endTime, limit, results);
    } catch (StreamNotFoundException e) {
      throw new IOException(e);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return results;
  }
}
