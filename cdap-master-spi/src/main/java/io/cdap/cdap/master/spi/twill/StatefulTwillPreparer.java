/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;

import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;

/**
 * Extension interface for {@link TwillPreparer} to implement if it supports stateful execution of
 * {@link TwillRunnable}.
 */
public interface StatefulTwillPreparer extends TwillPreparer {

  /**
   * Declares the given runnable with stateful execution.
   *
   * @param runnableName name of the {@link TwillRunnable}
   * @param orderedStart {@code true} to start replicas one by one; {@code false} to start replicas in parallel
   * @param statefulDisk an optional list of {@link StatefulDisk} available for the runnable container
   * @return this {@link TwillPreparer}
   */
  StatefulTwillPreparer withStatefulRunnable(String runnableName, boolean orderedStart, StatefulDisk... statefulDisk);
}
