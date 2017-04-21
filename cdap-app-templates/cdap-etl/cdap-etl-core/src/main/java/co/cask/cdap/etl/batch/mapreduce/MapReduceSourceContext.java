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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.log.LogContext;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * MapReduce Source Context. Delegates operations to MapReduce Context.
 */
public class MapReduceSourceContext extends MapReduceBatchContext implements BatchSourceContext {
  private final Set<String> inputNames;
  private final boolean isPreviewEnabled;

  public MapReduceSourceContext(MapReduceContext context, Metrics metrics, LookupProvider lookup,
                                Map<String, String> runtimeArgs, StageInfo stageInfo, boolean isPreviewEnabled) {
    super(context, metrics, lookup, runtimeArgs, stageInfo);
    this.inputNames = new HashSet<>();
    this.isPreviewEnabled = isPreviewEnabled;
  }

  @Override
  public void setInput(final Input input) {
    Input trackableInput = LogContext.runWithoutLoggingUnchecked(new Callable<Input>() {
      @Override
      public Input call() throws Exception {
        Input trackableInput =  ExternalDatasets.makeTrackable(mrContext.getAdmin(), suffixInput(input));
        mrContext.addInput(trackableInput);
        return trackableInput;
      }
    });
    inputNames.add(trackableInput.getAlias());
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  /**
   * @return set of inputs that were added
   */
  public Set<String> getInputNames() {
    return inputNames;
  }

  /**
   * Suffix the alias of {@link Input} so that aliases of inputs are unique.
   */
  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }
}
