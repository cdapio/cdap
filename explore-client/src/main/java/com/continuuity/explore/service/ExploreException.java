package com.continuuity.explore.service;

/**
 * Exception thrown by {@link Explore}.
 */
public class ExploreException extends Exception {
  public ExploreException(String s) {
    super(s);
  }

  public ExploreException(Throwable throwable) {
    super(throwable);
  }

  public ExploreException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
