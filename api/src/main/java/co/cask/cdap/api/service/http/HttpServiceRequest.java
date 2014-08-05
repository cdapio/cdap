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

package co.cask.cdap.api.service.http;

import com.google.common.collect.Multimap;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface which contains methods for processing HTTP requests.
 */
public interface HttpServiceRequest {

  /**
   * @return The method of this request.
   */
  String getMethod();

  /**
   * @return The URI of this request.
   */
  String getRequestURI();

  /**
   * @return The data content of this request.
   */
  ByteBuffer getContent();

  /**
   * @return The headers of this request. Where each header name can map to multiple values
   */
  Multimap<String, String> getHeaders();

  /**
   * Returns all of the values for a specified header.
   * @param key The header to find
   * @return List of all of the values for that header.
   */
  List<String> getHeaders(String key);

  /**
   * @param key The header to find
   * @return The value of the specified header. If the header maps to multiple values,
   * then the first value should be returned.
   */
  String getHeader(String key);
}
