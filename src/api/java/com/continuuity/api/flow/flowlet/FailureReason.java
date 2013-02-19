package com.continuuity.api.flow.flowlet;

import com.google.common.base.Objects;

/**
 *
 */
public final class FailureReason {

  /**
   * Specifies the type of errors that can be seen during
   * processing of input and while applying the operations
   * generated during the processing.
   */
  public enum Type {
    USER,
    IO_ERROR,
  }

  /**
   * Type of message.
   */
  private final Type type;

  /**
   * Textual description of error message.
   */
  private final String message;

  /**
   * Immutable object creation.
   * @param type of failure
   * @param message associated with failure.
   */
  public FailureReason(Type type, String message) {
    this.type = type;
    this.message = message;
  }

  /**
   * Returns the type of failure
   * @return type of failure
   */
  Type getType() {
    return this.type;
  }

  /**
   * Message associated with error.
   * @return string representation of error message.
   */
  String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("type", type)
      .add("message", message)
      .toString();
  }
}
