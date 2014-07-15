/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.common.http;

import com.continuuity.internal.io.ByteBufferInputStream;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents an HTTP request to be executed by {@link HttpRequests}.
 */
public class HttpRequest {

  private final HttpMethod method;
  private final URL url;
  private final Map<String, String> headers;
  private final InputSupplier<? extends InputStream> body;

  public HttpRequest(HttpMethod method, URL url, @Nullable Map<String, String> headers,
                     @Nullable InputSupplier<? extends InputStream> body) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
  }

  public static Builder get(URL url) {
    return new Builder(HttpMethod.GET, url);
  }

  public static Builder post(URL url) {
    return new Builder(HttpMethod.POST, url);
  }

  public static Builder delete(URL url) {
    return new Builder(HttpMethod.DELETE, url);
  }

  public static Builder put(URL url) {
    return new Builder(HttpMethod.PUT, url);
  }

  public static Builder builder(HttpMethod method, URL url) {
    return new Builder(method, url);
  }

  public HttpMethod getMethod() {
    return method;
  }

  public URL getURL() {
    return url;
  }

  @Nullable
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Nullable
  public InputSupplier<? extends InputStream> getBody() {
    return body;
  }

  /**
   * Builder for {@link HttpRequest}.
   */
  public static final class Builder {
    private HttpMethod method;
    private URL url;
    private Map<String, String> headers = Maps.newHashMap();
    private InputSupplier<? extends InputStream> body;

    Builder(HttpMethod method, URL url) {
      this.method = method;
      this.url = url;
    }

    public Builder addHeader(String key, String value) {
      this.headers.put(key, value);
      return this;
    }

    public Builder addHeaders(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder withBody(InputSupplier<? extends InputStream> body) {
      this.body = body;
      return this;
    }

    public Builder withBody(File body) {
      this.body = Files.newInputStreamSupplier(body);
      return this;
    }

    public Builder withBody(String body) {
      this.body = ByteStreams.newInputStreamSupplier(body.getBytes(Charsets.UTF_8));
      return this;
    }

    public Builder withBody(final ByteBuffer body) {
      this.body = new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return new ByteBufferInputStream(body);
        }
      };
      return this;
    }

    public HttpRequest build() {
      return new HttpRequest(method, url, headers, body);
    }
  }
}
