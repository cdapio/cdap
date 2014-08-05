/*
 * Copyright 2014 Cask, Inc.
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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link HttpServiceRequest} which binds the methods
 * to the appropriate methods for a {@link HttpRequest}.
 */
final class DefaultHttpServiceRequest implements HttpServiceRequest {

  private final HttpRequest request;
  private final ByteBuffer content;
  private final Multimap<String, String> headers;

  /**
   * Instantiates this class from a {@link HttpRequest}
   * @param request
   */
  DefaultHttpServiceRequest(HttpRequest request) {
    this.request = request;
    this.content = request.getContent().toByteBuffer();

    final ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    for (Map.Entry<String, String> header : request.getHeaders()) {
      builder.put(header);
    }
    this.headers = builder.build();
  }

  /**
   * @return The method of the request.
   */
  @Override
  public String getMethod() {
    return request.getMethod().toString();
  }

  /**
   * @return The URI of the request.
   */
  @Override
  public String getRequestURI() {
    return request.getUri();
  }

  /**
   * @return the data content of the request as a ByteBuffer.
   */
  @Override
  public ByteBuffer getContent() {
    return content.duplicate().asReadOnlyBuffer();
  }

  /**
   * @return The headers of this request. Where each header name can map to multiple values
   */
  @Override
  public Multimap<String, String> getHeaders() {
    return headers;
  }

  /**
   * Returns all of the values for a specified header.
   * @param key The header to find
   * @return List of all of the values for that header.
   */
  @Override
  public List<String> getHeaders(String key) {
    return ImmutableList.copyOf(headers.get(key));
  }

  /**
   * @param key The header to find
   * @return The value of the specified header. If the header maps to multiple values,
   * then the first value should be returned.
   */
  @Override
  public String getHeader(String key) {
    Collection<String> values = headers.get(key);
    return values.isEmpty() ? null : values.iterator().next();
  }
}
