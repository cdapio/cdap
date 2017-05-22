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

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.schedule.constraint.Constraint;

/**
 * A constraint that is checked before executing a schedule.
 */
public interface CheckableConstraint extends Constraint {

  /**
   * Checks a ConstraintContext against a program schedule.
   *
   * @param schedule the schedule that is being checked against
   * @param context context information for the check
   * @return the result of the check
   */
  ConstraintResult check(ProgramSchedule schedule, ConstraintContext context);
}
