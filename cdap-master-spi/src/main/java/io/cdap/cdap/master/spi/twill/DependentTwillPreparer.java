/*
 * Copyright © 2021 Cask Data, Inc.
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
 * Extension interface for {@link TwillPreparer} to specify dependent {@link TwillRunnable}.
 */
public interface DependentTwillPreparer extends TwillPreparer {

  /**
   * Specifies the main runnable name along with its dependent runnables that will go with it in the same pod.
   * @param mainRunnableName name of the main {@link TwillRunnable}
   * @param dependentRunnableName names of dependent {@link TwillRunnable}
   * @return this {@link TwillPreparer}
   */
  DependentTwillPreparer dependentRunnableNames(String mainRunnableName, String... dependentRunnableName);
}
