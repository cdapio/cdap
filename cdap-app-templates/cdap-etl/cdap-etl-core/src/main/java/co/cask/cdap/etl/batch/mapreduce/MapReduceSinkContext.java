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
import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * MapReduce Sink Context. Delegates operations to MapReduceContext and also keeps track of which outputs
 * were written to.
 */
public class MapReduceSinkContext extends MapReduceBatchContext implements BatchSinkContext {
  private final String aliasSuffix;
  private final Set<String> outputNames;

  private Output externalOutput;

  public MapReduceSinkContext(MapReduceContext context, Metrics metrics, LookupProvider lookup, String stageName,
                              Map<String, String> runtimeArgs, String aliasSuffix) {
    super(context, metrics, lookup, stageName, runtimeArgs);
    this.outputNames = new HashSet<>();
    this.aliasSuffix = aliasSuffix;
  }

  @Override
  public void addOutput(final String datasetName) {
    addOutput(datasetName, new HashMap<String, String>());
  }

  @Override
  public void addOutput(final String datasetName, final Map<String, String> arguments) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        externalOutput = suffixOutput(Output.ofDataset(datasetName, arguments));
        mrContext.addOutput(externalOutput);
        return null;
      }
    });
    outputNames.add(datasetName);
  }

  @Override
  public void addOutput(final String outputName, final OutputFormatProvider outputFormatProvider) {
    LogContext.runWithoutLoggingUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        externalOutput = suffixOutput(Output.of(outputName, outputFormatProvider));
        mrContext.addOutput(externalOutput);
        return null;
      }
    });
    outputNames.add(outputName);
  }

  @Override
  public void addOutput(final Output output) {
    Output trackableOutput = LogContext.runWithoutLoggingUnchecked(new Callable<Output>() {
      @Override
      public Output call() throws Exception {
        externalOutput = suffixOutput(output);
        Output trackableOutput = ExternalDatasets.makeTrackable(mrContext.getAdmin(), externalOutput);
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

  private Output suffixOutput(Output output) {
    if (Strings.isNullOrEmpty(aliasSuffix)) {
      return output;
    }

    String suffixedAlias = (output.getAlias() != null) ?
      String.format("%s.%s", output.getAlias(), aliasSuffix) : aliasSuffix;
    return output.alias(suffixedAlias);
  }

  @Override
  protected String getAlias() {
    return externalOutput.getAlias();
  }
}
