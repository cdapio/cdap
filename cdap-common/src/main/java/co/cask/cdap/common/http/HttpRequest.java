/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

import co.cask.common.ContentProvider;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.InputSupplier;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
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
  private final ContentProvider<? extends InputStream> body;
  private final Long bodyLength;

  public HttpRequest(HttpMethod method, URL url, @Nullable Multimap<String, String> headers,
                     @Nullable ContentProvider<? extends InputStream> body,
                     @Nullable Long bodyLength) {
    this.method = method;
    this.url = url;
    this.headers = headers;
    this.body = body;
    this.bodyLength = bodyLength;
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

  public static Builder builder(HttpRequest request) {
    return new Builder(request);
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
  public ContentProvider<? extends InputStream> getBody() {
    return body;
  }

  @Nullable
  public Long getBodyLength() {
    return bodyLength;
  }

  /**
   * Builder for {@link HttpRequest}.
   */
  public static final class Builder {
    private final HttpMethod method;
    private final URL url;
    private final Multimap<String, String> headers;
    private ContentProvider<? extends InputStream> body;
    private Long bodyLength;

    Builder(HttpMethod method, URL url) {
      this.method = method;
      this.url = url;
      this.headers = LinkedListMultimap.create();
    }

    public Builder(HttpRequest request) {
      this.method = request.method;
      this.url = request.url;
      this.headers = request.headers;
      this.body = request.body;
      this.bodyLength = request.bodyLength;
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
      return withBody(new ContentProvider<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return body.getInput();
        }
      });
    }

    public Builder withBody(ContentProvider<? extends InputStream> body) {
      this.body = body;
      this.bodyLength = null;
      return this;
    }

    public Builder withBody(File body) {
      Preconditions.checkNotNull(body);
      this.body = new ContentProvider() {
        @Override
        public InputStream getInput() throws IOException {
          return new FileInputStream(body);
        }
      };
      this.bodyLength = body.length();
      return this;
    }

    public Builder withBody(String body) {
      return withBody(body, Charsets.UTF_8);
    }

    public Builder withBody(String body, Charset charset) {
      byte[] bytes = body.getBytes(charset);
      this.body = new ContentProvider<InputStream>() {
        @Override
        public InputStream getInput() {
          return new ByteArrayInputStream(bytes, 0, bytes.length);
        }
      };
      this.bodyLength = (long) bytes.length;
      return this;
    }

    public Builder withBody(final ByteBuffer body) {
      final ByteBuffer duplicate = body.duplicate();
      this.body = new ContentProvider<InputStream>() {
        @Override
        public InputStream getInput() {
          return new ByteBufferInputStream(duplicate);
        }
      };
      this.bodyLength = (long) (duplicate.remaining());
      return this;
    }

    public HttpRequest build() {
      return new HttpRequest(method, url, headers, body, bodyLength);
    }
  }
}
