/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.api.common.HttpErrorStatusProvider;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;

import java.net.HttpURLConnection;

/**
 * Exception thrown when Authentication is successful, but a {@link Principal} is not authorized to perform an
 * {@link Action} on an {@link EntityId}.
 */
public class UnauthorizedException extends Exception implements HttpErrorStatusProvider {

  public UnauthorizedException(Principal principal, Action action, EntityId entityId) {
    super(String.format("Principal '%s' is not authorized to perform action '%s' on entity '%s'",
                        principal, action, entityId));
  }

  public UnauthorizedException(String message) {
    super(message);
  }

  @Override
  public int getStatusCode() {
    return HttpURLConnection.HTTP_FORBIDDEN;
  }
}
