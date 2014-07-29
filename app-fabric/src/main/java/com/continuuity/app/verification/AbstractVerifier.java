/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.app.verification;

import com.continuuity.error.Err;
import com.continuuity.proto.Id;
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
