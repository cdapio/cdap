package com.continuuity.passport.core.exceptions;

/**
 *
 */
public class VPCNotFoundException extends Exception {


  public VPCNotFoundException(String message) {
    super(message);
  }

  public VPCNotFoundException (String message, Exception cause) {
    super(message, cause);
  }

}
