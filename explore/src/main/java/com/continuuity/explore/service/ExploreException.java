package com.continuuity.explore.service;

/**
 * Exception thrown by {@link ExploreService}.
 */
public class ExploreException extends Exception {
  public ExploreException(String s) {
    super(s);
  }

  public ExploreException(Throwable throwable) {
    super(throwable);
  }
}
