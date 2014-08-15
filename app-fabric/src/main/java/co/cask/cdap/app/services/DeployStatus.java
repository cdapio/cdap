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

package co.cask.cdap.app.services;

/**
 * Defines the status of a resource.
 */
public enum DeployStatus {
  NOT_FOUND(0, "The archive was not found."),
  REGISTERED(1, "The archive has been registered."),
  UPLOADING(2, "The archive is being uploaded."),
  VERIFYING(3, "The archive is being verified."),
  FAILED(4, "There was a problem verifying the archive or its contents."),
  DEPLOYED(5, "Upload and verification completed successfully. Archive has been deployed."),
  UNDEPLOYED(6, "Archive has been un-deployed");

  private String message;
  private int code;

  DeployStatus(int code, String message) {
    this.code = code;
    this.message = message;
  }

  public int getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

  public static String getMessage(int code) {
    for (DeployStatus status : values()) {
      if (status.getCode() == code) {
        return status.name();
      }
    }
    return "";
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
