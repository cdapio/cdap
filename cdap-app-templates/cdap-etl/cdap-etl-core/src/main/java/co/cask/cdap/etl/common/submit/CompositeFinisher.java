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

package co.cask.cdap.etl.common.submit;

import co.cask.cdap.etl.api.batch.BatchConfigurable;

import java.util.ArrayList;
import java.util.List;

/**
 * A finisher of one or more {@link BatchConfigurable BatchConfigurables}.
 * If any of them throws an exception, the exception will be logged before moving on to finish the other objects.
 */
public class CompositeFinisher implements Finisher {
  private final List<Finisher> finishers;

  public CompositeFinisher(List<Finisher> finishers) {
    this.finishers = new ArrayList<>(finishers);
  }

  /**
   * Run logic on program finish.
   *
   * @param succeeded whether the program run succeeded or not
   */
  @Override
  public void onFinish(boolean succeeded) {
    for (Finisher finisher : finishers) {
      finisher.onFinish(succeeded);
    }
  }
}
