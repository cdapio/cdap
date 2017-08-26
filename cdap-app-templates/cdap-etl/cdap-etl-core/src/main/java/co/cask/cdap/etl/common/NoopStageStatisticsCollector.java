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

package co.cask.cdap.etl.common;

import java.io.Serializable;

/**
 * Implementation of {@link StageStatisticsCollector} which is no-op.
 * This implementation is used particularly in the Spark Streaming pipelines and possibly when
 * the collection of the stage level statistics is disabled.
 */
public class NoopStageStatisticsCollector implements StageStatisticsCollector, Serializable {
  private static final long serialVersionUID = -7897960584858589310L;
  
  @Override
  public void incrementInputRecordCount() {
    // no-op
  }

  @Override
  public void incrementOutputRecordCount() {
    // no-op
  }

  @Override
  public void incrementErrorRecordCount() {
    // no-op
  }
}
