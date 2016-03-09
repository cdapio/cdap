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

package co.cask.cdap.api.service.http;

import com.google.common.collect.Multimap;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Interface with methods for processing HTTP requests.
 */
public interface HttpServiceRequest {

  /**
   * @return the method of this request
   */
  String getMethod();

  /**
   * @return the URI of this request
   */
  String getRequestURI();

  /**
   * @return the data content of this request
   */
  ByteBuffer getContent();

  /**
   * @return the headers of this request; each header name can map to multiple values
   * @deprecated Use {@link #getAllHeaders()} instead. This method will be removed in future release.
   */
  @Deprecated
  Multimap<String, String> getHeaders();

  /**
   * @return all headers of this request; each header name can map to multiple values
   */
  Map<String, List<String>> getAllHeaders();

  /**
   * Returns all of the values for a specified header
   *
   * @param key the header to find
   * @return all of the values for that header
   */
  List<String> getHeaders(String key);

  /**
   * @param key the header to find
   * @return the value of the specified header; if the header maps to multiple values, return the first value
   */
  String getHeader(String key);
}
