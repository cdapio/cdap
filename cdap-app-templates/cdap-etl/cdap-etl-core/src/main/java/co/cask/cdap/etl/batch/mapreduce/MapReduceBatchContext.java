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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.batch.AbstractBatchContext;
import co.cask.cdap.etl.batch.preview.NullOutputFormatProvider;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.common.ExternalDatasets;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;
import co.cask.cdap.etl.planner.StageInfo;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Abstract implementation of {@link BatchContext} using {@link MapReduceContext}.
 */
public class MapReduceBatchContext extends AbstractBatchContext implements BatchSinkContext, BatchSourceContext {
  protected final MapReduceContext mrContext;
  private final boolean isPreviewEnabled;
  private final Set<String> outputNames;
  private final Set<String> inputNames;
  private final Caller caller;

  public MapReduceBatchContext(MapReduceContext context, Metrics metrics, StageInfo stageInfo) {
    super(context, metrics, new DatasetContextLookupProvider(context), context.getLogicalStartTime(),
          context.getAdmin(), stageInfo, new BasicArguments(context));
    this.mrContext = context;
    this.caller = NoStageLoggingCaller.wrap(Caller.DEFAULT);
    this.outputNames = new HashSet<>();
    this.inputNames = new HashSet<>();
    this.isPreviewEnabled = context.getDataTracer(stageInfo.getName()).isEnabled();
  }

  @Override
  public <T> T getHadoopJob() {
    return caller.callUnchecked(new Callable<T>() {
      @Override
      public T call() {
        return mrContext.getHadoopJob();
      }
    });
  }


  @Override
  public void setInput(final Input input) {
    Input trackableInput = caller.callUnchecked(new Callable<Input>() {
      @Override
      public Input call() throws Exception {
        Input trackableInput = ExternalDatasets.makeTrackable(mrContext.getAdmin(), suffixInput(input));
        mrContext.addInput(trackableInput);
        return trackableInput;
      }
    });
    inputNames.add(trackableInput.getAlias());
  }

  @Override
  public void addOutput(final String datasetName) {
    addOutput(datasetName, Collections.<String, String>emptyMap());
  }

  @Override
  public void addOutput(final String datasetName, final Map<String, String> arguments) {
    String alias = caller.callUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(getOutput(Output.ofDataset(datasetName, arguments)));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final String outputName, final OutputFormatProvider outputFormatProvider) {
    String alias = caller.callUnchecked(new Callable<String>() {
      @Override
      public String call() throws Exception {
        Output output = suffixOutput(getOutput(Output.of(outputName, outputFormatProvider)));
        mrContext.addOutput(output);
        return output.getAlias();
      }
    });
    outputNames.add(alias);
  }

  @Override
  public void addOutput(final Output output) {
    final Output actualOutput = suffixOutput(getOutput(output));
    Output trackableOutput = caller.callUnchecked(new Callable<Output>() {
      @Override
      public Output call() throws Exception {
        Output trackableOutput = isPreviewEnabled ? actualOutput : ExternalDatasets.makeTrackable(mrContext.getAdmin(),
                                                                                                  actualOutput);
        mrContext.addOutput(trackableOutput);
        return trackableOutput;
      }
    });
    outputNames.add(trackableOutput.getAlias());
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

  /**
   * Suffix the alias of {@link Input} so that aliases of inputs are unique.
   */
  private Input suffixInput(Input input) {
    String suffixedAlias = String.format("%s-%s", input.getAlias(), UUID.randomUUID());
    return input.alias(suffixedAlias);
  }

  /**
   * Get the output, if preview is enabled, return the output with a {@link NullOutputFormatProvider}.
   */
  private Output getOutput(Output output) {
    if (isPreviewEnabled) {
      return Output.of(output.getName(), new NullOutputFormatProvider());
    }
    return output;
  }
}
