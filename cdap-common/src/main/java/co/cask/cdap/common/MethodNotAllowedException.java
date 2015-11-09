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

package co.cask.cdap.common;

import org.jboss.netty.handler.codec.http.HttpMethod;

/**
 * Thrown when some method is not allowed.
 */
public class MethodNotAllowedException extends Exception {

  private final HttpMethod method;
  private final String apiPath;

  public MethodNotAllowedException(HttpMethod method, String apiPath) {
    super(String.format("Method %s is not allowed on path %s.", method, apiPath));
    this.method = method;
    this.apiPath = apiPath;
  }

  public HttpMethod getMethod() {
    return method;
  }

  public String getApiPath() {
    return apiPath;
  }
}
