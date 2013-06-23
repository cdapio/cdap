/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.verification;

import com.google.common.base.CharMatcher;

/**
 *
 */
public abstract class AbstractVerifier {

  protected boolean isId(final String name) {
    return CharMatcher.inRange('A', 'Z')
             .or(CharMatcher.inRange('a', 'z'))
             .or(CharMatcher.is('-'))
             .or(CharMatcher.is('_'))
             .or(CharMatcher.inRange('0', '9')).matchesAllOf(name);
  }
}
