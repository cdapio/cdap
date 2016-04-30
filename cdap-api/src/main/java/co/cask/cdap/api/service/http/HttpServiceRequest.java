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

package co.cask.cdap.api.service.http;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

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
   * @return all headers of this request; each header name can map to multiple values
   */
  Map<String, List<String>> getAllHeaders();

  /**
   * Returns all of the values for a specified header
   *
   * @param key the header to find
   * @return all of the values for that header; an empty list will be returned if there is no such header
   */
  List<String> getHeaders(String key);

  /**
   * @param key the header to find
   * @return the value of the specified header; if the header maps to multiple values, return the first value;
   *         if there is no such header, a {@code null} value will be returned.
   */
  @Nullable
  String getHeader(String key);
}
