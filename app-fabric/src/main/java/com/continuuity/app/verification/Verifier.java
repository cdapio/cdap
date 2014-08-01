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

import com.continuuity.proto.Id;

/**
 * A verifier for verifying the specifications provided.
 * <p>
 * Implementors of this interface will take Specification as input
 * and verify the information within the specification meets satisfies
 * all the checkpoints.
 * </p>
 * <p/>
 * <p>
 * Implementation of this interface are expected to be thread-safe,
 * an can be safely accessed by multiple concurrent threads.
 * </p>
 *
 * @param <T> Type of object to be verified.
 */
public interface Verifier<T> {

  /**
   * Verifies <code>input</code> and returns {@link VerifyResult}
   * containing the status of verification.
   *
   * @param appId the application where this is verified
   * @param input to be verified
   * @return An instance of {@link VerifyResult}
   */
  VerifyResult verify(Id.Application appId, T input);
}
