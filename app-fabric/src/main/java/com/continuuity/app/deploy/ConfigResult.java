/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

/**
 * A container class for holding configuration result.
 *
 * @param <T> Type of specification this result will hold.
 */
public class ConfigResult<T> {
  private final T specification;
  private final boolean status;
  private final String message;

  public ConfigResult(String message) {
    this(null, false, message);
  }

  public ConfigResult(T specification, boolean status, String message) {
    this.specification = specification;
    this.status = status;
    this.message = message;
  }

  public T getSpecification() {
    return specification;
  }

  public boolean getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }
}
