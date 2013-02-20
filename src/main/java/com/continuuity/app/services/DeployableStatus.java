package com.continuuity.app.services;

import com.google.common.base.Objects;

/**
 * Defines the status of a resource.
 */
public final class DeployableStatus {
  public transient static int NOT_FOUND = 0;
  public transient static int REGISTERED = 1;
  public transient static int UPLOADING = 2;
  public transient static int VERIFYING = 3;
  public transient static int VERIFICATION_FAILURE = 4;
  public transient static int DEPLOYED = 5;
  public transient static int UNDEPLOY = 6;
  public transient static int ALREADY_RUNNING = 7; // TODO Should NOT be here?

  private transient static String[] BASE_REASON = {
    "Resource not found.",
    "Resource has been registered.",
    "Resource is being uploaded",
    "Verification of resource has begun.",
    "Verification of resource has failed.",
    "Upload & verification were completed successfully. Resource is now deployed.",
    "Resource has been undeployed",
    "One of the flow in the resource is already running. Please stop it first"
  };

  private final int code;
  private final String specifics;

  private DeployableStatus(int code, String specifics) {
    this.code = code;
    this.specifics = specifics;
  }

  private DeployableStatus(int status) {
    this.code = status;
    this.specifics = "";
  }

  public int getCode() {
    return code;
  }

  public String getMessage() {
    if("".equals(specifics)) {
      return String.format("%s", BASE_REASON[code]);
    }
    return String.format("%s", specifics);
  }

  public String getReason() {
    return BASE_REASON[code];
  }

  public String getSpecifics() {
    return specifics;
  }

  public static DeployableStatus newStatus(int status) {
    return new DeployableStatus(status);
  }

  public static DeployableStatus newStatus(int status, String specifics) {
    return new DeployableStatus(status, specifics);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("code", code)
      .add("specifies", specifics)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if(other == null) {
      return false;
    }

    if (!(other instanceof DeployableStatus)) {
      return false;
    }

    DeployableStatus that = (DeployableStatus)other;
    return Objects.equal(code, that.code) &&
      Objects.equal(specifics, that.specifics);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(code, specifics);
  }
}
