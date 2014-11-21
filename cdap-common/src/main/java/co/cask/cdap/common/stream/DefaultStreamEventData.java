/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.stream;

import co.cask.cdap.api.stream.StreamEventData;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default implementation of {@link co.cask.cdap.api.stream.StreamEventData} that takes headers and body from
 * constructor.
 */
public class DefaultStreamEventData implements StreamEventData {

  private final Map<String, String> headers;
  private final ByteBuffer body;

  public DefaultStreamEventData(Map<String, String> headers, ByteBuffer body) {
    this.headers = ImmutableMap.copyOf(headers);
    this.body = body;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public ByteBuffer getBody() {
    return body;
  }
}
