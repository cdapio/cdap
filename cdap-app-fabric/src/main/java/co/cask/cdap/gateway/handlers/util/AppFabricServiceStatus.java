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

package co.cask.cdap.gateway.handlers.util;

import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Report status for app fabric services
 */
public final class AppFabricServiceStatus {

  protected static final AppFabricServiceStatus OK = new AppFabricServiceStatus(HttpResponseStatus.OK, "");

  protected static final AppFabricServiceStatus PROGRAM_STILL_RUNNING =
    new AppFabricServiceStatus(HttpResponseStatus.FORBIDDEN, "Program is still running");

  protected static final AppFabricServiceStatus PROGRAM_ALREADY_RUNNING =
    new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program is already running");

  protected static final AppFabricServiceStatus PROGRAM_ALREADY_STOPPED =
    new AppFabricServiceStatus(HttpResponseStatus.CONFLICT, "Program already stopped");

  protected static final AppFabricServiceStatus RUNTIME_INFO_NOT_FOUND =
    new AppFabricServiceStatus(HttpResponseStatus.CONFLICT,
                               UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND));

  protected static final AppFabricServiceStatus PROGRAM_NOT_FOUND =
    new AppFabricServiceStatus(HttpResponseStatus.NOT_FOUND, "Program not found");

  protected static final AppFabricServiceStatus INTERNAL_ERROR =
    new AppFabricServiceStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error");

  private final HttpResponseStatus code;
  private final String message;

  /**
   * Describes the output status of app fabric operations.
   */
  protected AppFabricServiceStatus(HttpResponseStatus code, String message) {
    this.code = code;
    this.message = message;
  }

  public HttpResponseStatus getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
