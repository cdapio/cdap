package com.continuuity.app.verification;

import com.continuuity.error.Errors;

/**
 * This class defines the result of {@link Verifier#verify(Object)}.
 */
public final class VerifyResult {
  /**
   * Status of verification.
   */
  public static enum Status {
    SUCCESS,
    FAILED
  }

  ;

  /**
   * Stores status of verification.
   */
  private final Status status;

  /**
   * Descriptive message in case of failure.
   */
  private final String message;

  /**
   * Constructor.
   *
   * @param status  of the {@link Verifier#verify(Object)}
   * @param message description in case of failure.
   */
  public VerifyResult(Status status, String message) {
    this.message = message;
    this.status = status;
  }

  /**
   * @return Status of message.
   */
  public Status getStatus() {
    return status;
  }

  public boolean isSuccess() {
    return status == Status.SUCCESS;
  }

  /**
   * @return Descriptive message in case of failure.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Helper static method for creating successfull {@link VerifyResult}.
   *
   * @return An instance of {@link VerifyResult} which is successful.
   */
  public static VerifyResult success() {
    return new VerifyResult(Status.SUCCESS, "OK");
  }

  /**
   * Helper static method for creating failure {@link VerifyResult} with
   * a descriptive message.
   *
   * @return An instance of {@link VerifyResult} which has failed with descriptive message.
   */
  public static VerifyResult failure(Errors error, Object... objects) {
    return new VerifyResult(Status.FAILED, error.getMessage(objects));
  }
}
