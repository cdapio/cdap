package com.continuuity.passport.core;

/*
 * Abstract class that defines Credentials that qualify an {@code Entity}
 * Designed to be used for userCredentials, dataSetCrendentials   etc
 */

public abstract class Credentials {

  private String type;

  public String getClientType() {
    return type;
  }

  public void setClientType(String type) {
    this.type = type;
  }

}
