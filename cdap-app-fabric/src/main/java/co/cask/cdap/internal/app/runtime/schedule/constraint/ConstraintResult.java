/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import javax.annotation.Nullable;

/**
 * The result of a Constraint check. Indicates whether it was satisfied or not, and if not, the duration that
 * the constraint will likely not be satisfied for.
 */
public class ConstraintResult {
  public static final ConstraintResult SATISFIED = new ConstraintResult(SatisfiedState.SATISFIED, null);

  enum SatisfiedState {
    SATISFIED, NOT_SATISFIED, NEVER_SATISFIED
  }
  private final SatisfiedState satisfiedState;
  private final Long millisBeforeNextRetry;

  ConstraintResult(SatisfiedState satisfiedState) {
    this(satisfiedState, null);
  }

  ConstraintResult(SatisfiedState satisfiedState, @Nullable Long millisBeforeNextRetry) {
    this.satisfiedState = satisfiedState;
    this.millisBeforeNextRetry = millisBeforeNextRetry;
  }

  public SatisfiedState getSatisfiedState() {
    return satisfiedState;
  }

  @Nullable
  public Long getMillisBeforeNextRetry() {
    return millisBeforeNextRetry;
  }
}
