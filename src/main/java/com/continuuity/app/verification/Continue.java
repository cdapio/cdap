/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.verification;

/**
 *
 */
public interface Continue<T> {
  public void completed(T result);
}
