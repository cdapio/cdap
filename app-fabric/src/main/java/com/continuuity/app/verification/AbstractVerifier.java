/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.verification;

import com.continuuity.app.Id;
import com.continuuity.error.Err;
import com.google.common.base.CharMatcher;

/**
 * @param <T> Type of thing to get verified.
 */
public abstract class AbstractVerifier<T> implements Verifier<T> {

  protected boolean isId(final String name) {
    return CharMatcher.inRange('A', 'Z')
             .or(CharMatcher.inRange('a', 'z'))
             .or(CharMatcher.is('-'))
             .or(CharMatcher.is('_'))
             .or(CharMatcher.inRange('0', '9')).matchesAllOf(name);
  }

  @Override
  public VerifyResult verify(Id.Application appId, T input) {
    // Checks if DataSet name is an ID
    String name = getName(input);
    if (!isId(name)) {
      return VerifyResult.failure(Err.NOT_AN_ID, name);
    }
    return VerifyResult.success();

  }

  protected abstract String getName(T input);
}
