/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import com.google.common.util.concurrent.Service;

/**
 * A {@link ProgramController} for {@link Spark} jobs. This class acts as an adapter for reflecting state changes
 * happening in {@link SparkRuntimeService}
 */
public final class SparkProgramController extends ProgramControllerServiceAdapter {

  private final SparkContext context;

  SparkProgramController(Service sparkRuntimeService, BasicSparkContext context) {
    super(sparkRuntimeService, context.getProgramName(), context.getRunId());
    this.context = context;
  }

  @Override
  protected boolean propagateServiceError() {
    // Don't propagate Spark failure as failure. Similar reason as in MapReduce case (CDAP-749).
    return false;
  }

  /**
   * Returns the {@link SparkContext} for Spark run represented by this controller.
   */
  public SparkContext getContext() {
    return context;
  }
}
