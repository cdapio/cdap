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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.io.ByteBuffers;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A mutable {@link StreamEventData} that allows setting header and body.
 */
@NotThreadSafe
public final class MutableStreamEventData extends StreamEventData {

  private ByteBuffer body;
  private Map<String, String> headers;

  public MutableStreamEventData() {
    super(ImmutableMap.<String, String>of(), ByteBuffers.EMPTY_BUFFER);
    body = ByteBuffers.EMPTY_BUFFER;
    headers = ImmutableMap.of();
  }

  @Override
  public ByteBuffer getBody() {
    return body;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  public MutableStreamEventData setBody(ByteBuffer body) {
    this.body = body;
    return this;
  }

  public MutableStreamEventData setHeaders(Map<String, String> headers) {
    this.headers = headers;
    return this;
  }
}
