/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.workflow;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface to represent the data that is transferred from one node to the next node in the {@link Workflow}
 */
public interface WorkflowToken {
  /**
   * Get the Hadoop counters from the previous MapReduce program in the Workflow. The method returns null
   * if the counters are not set.
   * @return the Hadoop MapReduce counters set by the previous MapReduce program
   */
  @Nullable
  public Map<String, Map<String, Long>> getMapReduceCounters();
}
