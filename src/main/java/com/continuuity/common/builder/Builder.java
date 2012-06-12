package com.continuuity.common.builder;

/**
 * Interface for specifying the implementor is a builder.
 */
public interface Builder<T> {
  /**
   * Constructs an object of type T.
   * @return instance of type T
   * @throws BuilderException
   */
  T build() throws BuilderException;
}
