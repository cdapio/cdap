package com.continuuity.common.builder;

/**
 * Exception that is raised when there is an issue building the object.
 */
public class BuilderException extends RuntimeException {
  /**
   * Basic construction of exception.
   */
  public BuilderException() {
    super();
  }

  /**
   * Construction of exception with reason specified.
   * @param reason for why the exception was thrown.
   */
  public BuilderException(String reason) {
    super(reason);
  }

  /**
   * Construction of exception with a {@link Throwable}.
   * @param throwable instance.
   */
  public BuilderException(Throwable throwable) {
    super(throwable);
  }

  /**
   * Construction of exception with reason and throwable.
   * @param reason   for why the exception is being thrown.
   * @param throwable instance.
   */
  public BuilderException(String reason, Throwable throwable) {
    super(reason, throwable);
  }
}
