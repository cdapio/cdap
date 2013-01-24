package com.continuuity.passport.core.service;

/**
 * Status Object that is used in the interfaces
 */
public class Status {
  public enum Type {OK,FAILED};

  private Type type;
  private String message;

  public Status () {
    this.type = Type.OK;
  }

  public Status(Type type,String message) {
    this.message = message;
    this.type = type;
  }
}
