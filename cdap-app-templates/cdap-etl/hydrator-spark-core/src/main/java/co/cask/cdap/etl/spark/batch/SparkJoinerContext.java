/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.spark.batch;

import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.batch.AbstractJoinerContext;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.planner.StageInfo;

/**
 * Spark joiner context for preparing jobs.
 */
public class SparkJoinerContext extends AbstractJoinerContext {

  protected SparkJoinerContext(StageInfo stageInfo, SparkClientContext scc) {
    super(scc, scc, scc.getMetrics(), new DatasetContextLookupProvider(scc),
          scc.getLogicalStartTime(), scc.getRuntimeArguments(), scc.getAdmin(), stageInfo);
  }

  @Override
  public <T> T getHadoopJob() {
    throw new UnsupportedOperationException();
  }
}
