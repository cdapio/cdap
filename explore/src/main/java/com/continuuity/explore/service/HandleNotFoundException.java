package com.continuuity.explore.service;

/**
 * Exception thrown when {@link Handle} is not found.
 */
public class HandleNotFoundException extends Exception {
  public HandleNotFoundException(String s) {
    super(s);
  }
}
