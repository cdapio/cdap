/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.service.http.HttpServiceRequest;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link HttpServiceRequest} which delegates calls to
 * the HttpServiceRequest's methods to the matching methods for a {@link HttpRequest}.
 */
final class DefaultHttpServiceRequest implements HttpServiceRequest {

  private final HttpRequest request;
  private final ByteBuf content;
  private final Map<String, List<String>> headers;

  /**
   * Instantiates the class from a {@link HttpRequest}
   *
   * @param request the request which will be bound to.
   */
  DefaultHttpServiceRequest(HttpRequest request) {
    this.request = request;
    this.content = request instanceof FullHttpRequest ? ((FullHttpRequest) request).content() : Unpooled.EMPTY_BUFFER;

    Map<String, List<String>> headers = new HashMap<>();
    for (String name : request.headers().names()) {
      headers.put(name, Collections.unmodifiableList(new ArrayList<>(request.headers().getAll(name))));
    }
    this.headers = Collections.unmodifiableMap(headers);
  }

  /**
   * @return the method of the request
   */
  @Override
  public String getMethod() {
    return request.getMethod().toString();
  }

  /**
   * @return the URI of the request
   */
  @Override
  public String getRequestURI() {
    return request.getUri();
  }

  /**
   * @return the data content of the request as a ByteBuffer
   */
  @Override
  public ByteBuffer getContent() {
    return content.duplicate().asReadOnly().nioBuffer();
  }

  @Override
  public Map<String, List<String>> getAllHeaders() {
    return headers;
  }

  /**
   * Returns all of the values for a specified header.
   *
   * @param key the header to find
   * @return all of the values for the header with the specified key
   */
  @Override
  public List<String> getHeaders(String key) {
    List<String> values = headers.get(key);
    return values == null ? ImmutableList.<String>of() : values;
  }

  /**
   * Returns the first value for the specified header.
   *
   * @param key the header to find
   * @return the value of the specified header; if the header maps to multiple values,
   * then the first value is returned
   */
  @Override
  public String getHeader(String key) {
    Collection<String> values = getHeaders(key);
    return values.isEmpty() ? null : values.iterator().next();
  }
}
