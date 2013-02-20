/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.services;

/**
 * Defines the status of a resource.
 */
public enum DeployStatus {
  NOT_FOUND(0, "Archive not found"),
  REGISTERED(1, "Archive has been registered"),
  UPLOADING(2, "Archive is being uploaded"),
  VERIFYING(3, "Archive is being verified"),
  FAILED(4, "Failed in verification & registration of Archive"),
  DEPLOYED(5, "Upload & verification were completed successfully. Archive is now deployed."),
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
}
