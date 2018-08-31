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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Return type for http requests executed by {@link HttpResponse}
 */
public class HttpResponse {
  private final int responseCode;
  private final String responseMessage;
  private final byte[] responseBody;
  private final Multimap<String, String> headers;

  HttpResponse(int responseCode, String responseMessage,
               byte[] responseBody, Map<String, List<String>> headers) {
    this(responseCode, responseMessage, responseBody, parseHeaders(headers));
  }

  HttpResponse(int responseCode, String responseMessage,
               byte[] responseBody, Multimap<String, String> headers) {
    this.responseCode = responseCode;
    this.responseMessage = responseMessage;
    this.responseBody = responseBody;
    this.headers = headers;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  public byte[] getResponseBody() {
    return responseBody;
  }

  public String getResponseBodyAsString() {
    return new String(responseBody, Charsets.UTF_8);
  }

  public String getResponseBodyAsString(Charset charset) {
    return new String(responseBody, charset);
  }

  public Multimap<String, String> getHeaders() {
    return headers;
  }

  private static Multimap<String, String> parseHeaders(Map<String, List<String>> headers) {
    ImmutableListMultimap.Builder<String, String> builder = new ImmutableListMultimap.Builder<String, String>();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      // By default, headers created by URLConnection contain an entry from null -> HTTP Response message
      if (entry.getKey() != null) {
        builder.putAll(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return String.format("Response code: %s, message: '%s', body: '%s'",
                         this.getResponseCode(), this.getResponseMessage(),
                         this.getResponseBody() == null ? "null" : this.getResponseBodyAsString());
  }
}
