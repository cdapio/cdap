package com.continuuity.passport.core;

/**
 * Created with IntelliJ IDEA.
 * User: sree
 * Date: 1/21/13
 * Time: 2:04 PM
 * To change this template use File | Settings | File Templates.
 */

/*
 * Abstract class that defines Credentials that qualify an {@code Entity}
 * Designed to be used for userCredentials, dataSetCrendentials   etc
 */

public abstract class Credentials  {

  private String type;

  public String getClientType() {
    return type;
  }

  public void setClientType(String type){
    this.type = type;
  }

}
