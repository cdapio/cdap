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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.log.LogContext;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * MapReduce Sink Context. Delegates operations to MapReduceContext and also keeps track of which outputs
 * were written to.
 */
public class MapReduceSinkContext extends MapReduceBatchContext implements BatchSinkContext {
  private final Set<String> outputNames;

  public MapReduceSinkContext(MapReduceContext context, Metrics metrics, LookupProvider lookup,
                              Map<String, String> runtimeArgs, StageInfo stageInfo) {
    super(context, metrics, lookup, runtimeArgs, stageInfo);
    this.outputNames = new HashSet<>();
  }

  @Override
  public void addOutput(final String datasetName) {
    addOutput(datasetName, Collections.<String, String>emptyMap());
  }

  @Override
  public void addOutput(final String datasetName, final Map<String, String> arguments) {
    String alias = LogContext.runWithoutLoggingUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(Output.ofDataset(datasetName, arguments));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final String outputName, final OutputFormatProvider outputFormatProvider) {
    String alias = LogContext.runWithoutLoggingUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(Output.of(outputName, outputFormatProvider));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final Output output) {
    Output trackableOutput = LogContext.runWithoutLoggingUnchecked(new Callable<Output>() {
      @Override
      public Output call() throws Exception {
        Output trackableOutput = ExternalDatasets.makeTrackable(mrContext.getAdmin(), suffixOutput(output));
        mrContext.addOutput(trackableOutput);
        return trackableOutput;
      }
    });
    outputNames.add(trackableOutput.getAlias());
  }

  /**
   * @return set of outputs that were added
   */
  public Set<String> getOutputNames() {
    return outputNames;
  }

  /**
   * Suffix the alias of {@link Output} so that aliases of outputs are unique.
   */
  private Output suffixOutput(Output output) {
    String suffixedAlias = String.format("%s-%s", output.getAlias(), UUID.randomUUID());
    return output.alias(suffixedAlias);
  }
}
