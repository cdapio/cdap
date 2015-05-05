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
import co.cask.cdap.test.StreamWriter;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @deprecated Use {@link RemoteStreamManager} instead
 */
@Deprecated
public class RemoteStreamWriter implements StreamWriter {

  private final RemoteStreamManager remoteStreamManager;

  public RemoteStreamWriter(RemoteStreamManager remoteStreamManager) {
    this.remoteStreamManager = remoteStreamManager;
  }

  @Override
  public void createStream() throws IOException {
    remoteStreamManager.createStream();
  }

  @Override
  public void send(String content) throws IOException {
    remoteStreamManager.send(content);
  }

  @Override
  public void send(byte[] content) throws IOException {
    remoteStreamManager.send(content);
  }

  @Override
  public void send(byte[] content, int off, int len) throws IOException {
    remoteStreamManager.send(new String(content, off, len, Charsets.UTF_8));
  }

  @Override
  public void send(ByteBuffer buffer) throws IOException {
    remoteStreamManager.send(Bytes.toString(buffer));
  }

  @Override
  public void send(Map<String, String> headers, String content) throws IOException {
    remoteStreamManager.send(headers, content);
  }

  @Override
  public void send(Map<String, String> headers, byte[] content) throws IOException {
    remoteStreamManager.send(headers, content);
  }

  @Override
  public void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException {
    remoteStreamManager.send(headers, content, off, len);
  }

  @Override
  public void send(Map<String, String> headers, ByteBuffer buffer) throws IOException {
    remoteStreamManager.send(headers, buffer);
  }
}
