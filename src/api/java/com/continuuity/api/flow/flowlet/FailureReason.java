package com.continuuity.api.flow.flowlet;

import com.google.common.base.Objects;

/**
 * This class carries information about the reason of failure.
 */
public final class FailureReason {

  /**
   * Specifies the type of errors that can be seen during
   * processing of input and while applying the operations
   * generated during the processing.
   */
  public enum Type {
    /**
     * Failure triggered by exception being thrown from the process method.
     */
    USER,

    /**
     * Error related to read/write of {@link com.continuuity.api.data.DataSet DataSet} or writing output.
     */
    IO_ERROR,
  }

  /**
   * Type of the failure.
   */
  private final Type type;

  /**
   * Textual description of error message.
   */
  private final String message;

  /**
   * Cause of the failure if it is from exception.
   */
  private final Throwable cause;

  /**
   * Immutable object creation.
   * @param type of failure
   * @param message associated with failure.
   * @param t cause of the failure.
   */
  public FailureReason(Type type, String message, Throwable t) {
    this.type = type;
    this.message = message;
    this.cause = t;
  }

  /**
   * Returns the type of failure
   * @return type of failure
   */
  public Type getType() {
    return this.type;
  }

  /**
   * Message associated with error.
   * @return string representation of error message.
   */
  public String getMessage() {
    return message;
  }

  /**
   * Cause of the error if it is caused by exceptions.
   *
   * @return The {@link Throwable} cause.
   */
  public Throwable getCause() {
    return cause;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("type", type)
      .add("message", message)
      .toString();
  }
}
