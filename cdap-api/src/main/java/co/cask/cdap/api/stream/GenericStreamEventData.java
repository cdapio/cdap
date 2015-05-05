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
package co.cask.cdap.api.stream;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Represents data in one stream event with the body as a specific type.
 *
 * @param <T> Type of the stream body.
 */
@Nonnull
public class GenericStreamEventData<T> {

  private final T body;
  private final Map<String, String> headers;

  /**
   * Constructs a {@link GenericStreamEventData} instance. The given body and headers are taken as-is.
   *
   * @param headers Map of key/value pairs of the event data
   * @param body body of the event data
   */
  public GenericStreamEventData(Map<String, String> headers, T body) {
    this.body = body;
    this.headers = ImmutableMap.copyOf(headers);
  }

  /**
   * @return An immutable map of all headers included in this event.
   */
  public Map<String, String> getHeaders() {
    return headers;
  }

  /**
   * @return The payload of the event.
   */
  public T getBody() {
    return body;
  }
}
