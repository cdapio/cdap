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
package co.cask.cdap.common.http;

import co.cask.cdap.internal.io.ByteBufferInputStream;
import com.google.common.base.Charsets;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents an HTTP request to be executed by {@link HttpRequests}.
 */
public class HttpRequest {

  private final HttpMethod method;
  private final URL url;
  private final Multimap<String, String> headers;
  private final InputSupplier<? extends InputStream> body;

  public HttpRequest(HttpMethod method, URL url, @Nullable Multimap<String, String> headers,
                     @Nullable InputSupplier<? extends InputStream> body) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
  }

  public static Builder get(URL url) {
    return builder(HttpMethod.GET, url);
  }

  public static Builder post(URL url) {
    return builder(HttpMethod.POST, url);
  }

  public static Builder delete(URL url) {
    return builder(HttpMethod.DELETE, url);
  }

  public static Builder put(URL url) {
    return builder(HttpMethod.PUT, url);
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
  public Multimap<String, String> getHeaders() {
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
    private final HttpMethod method;
    private final URL url;
    private final Multimap<String, String> headers = LinkedListMultimap.create();
    private InputSupplier<? extends InputStream> body;

    Builder(HttpMethod method, URL url) {
      this.method = method;
      this.url = url;
    }

    public Builder addHeader(String key, String value) {
      this.headers.put(key, value);
      return this;
    }

    public Builder addHeaders(@Nullable Multimap<String, String> headers) {
      if (headers != null) {
        this.headers.putAll(headers);
      }
      return this;
    }

    public Builder addHeaders(@Nullable Map<String, String> headers) {
      if (headers != null) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          this.headers.put(entry.getKey(), entry.getValue());
        }
      }
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

    public Builder withBody(String body, Charset charset) {
      this.body = ByteStreams.newInputStreamSupplier(body.getBytes(charset));
      return this;
    }

    public Builder withBody(final ByteBuffer body) {
      this.body = new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return new ByteBufferInputStream(body.duplicate());
        }
      };
      return this;
    }

    public HttpRequest build() {
      return new HttpRequest(method, url, headers, body);
    }
  }
}
