/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.application;

import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.IOException;
import java.net.URL;

/**
 * Common implementation of methods to interact with app fabric service over HTTP.
 */
public abstract class AbstractApplicationClient {

  /**
   * Executes an HTTP request.
   */
  protected abstract HttpResponse execute(HttpRequest request, int... allowedErrorCodes)
    throws IOException, UnauthenticatedException, UnauthorizedException;

  /**
   * Resolved the specified URL
   */
  protected abstract URL resolve(String resource) throws IOException;

}
