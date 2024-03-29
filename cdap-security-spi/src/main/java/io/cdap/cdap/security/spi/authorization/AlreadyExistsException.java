/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.proto.security.Role;
import java.net.HttpURLConnection;

/**
 * Exception thrown when a {@link Role} or an entity already exists
 */
public class AlreadyExistsException extends AccessException implements HttpErrorStatusProvider {

  public AlreadyExistsException(Role role) {
    super(String.format("%s already exists.", role));
  }

  public AlreadyExistsException(String message) {
    super(message);
  }

  @Override
  public int getStatusCode() {
    return HttpURLConnection.HTTP_CONFLICT;
  }
}
