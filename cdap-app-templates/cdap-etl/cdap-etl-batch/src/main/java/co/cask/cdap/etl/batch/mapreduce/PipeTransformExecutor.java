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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.batch.PipeTransformDetail;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.LocationAwareMDCWrapperLogger;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Executes chain of transforms
 * @param <IN> Type of input
 */
public class PipeTransformExecutor<IN> implements Destroyable {
  private final Set<String> startingPoints;
  private final Map<String, PipeTransformDetail> transformDetailMap;

  public PipeTransformExecutor(Map<String, PipeTransformDetail> transformDetailMap, Set<String> startingPoints) {
    this.transformDetailMap = transformDetailMap;
    this.startingPoints = startingPoints;
  }

  public void runOneIteration(IN input) {
    for (String stageName : startingPoints) {
      PipeTransformDetail transformDetail = transformDetailMap.get(stageName);
      transformDetail.process(new KeyValue<String, Object>(stageName, input));
    }
  }

  @Override
  public void destroy() {
    for (Map.Entry<String, PipeTransformDetail> entry : transformDetailMap.entrySet()) {
      entry.getValue().destroy();
    }
  }
}
