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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to create external datasets to track external sources/sinks.
 */
public final class ExternalDatasets {
  private static final String NAME_PREFIX = "hydrator-ext-";

  /**
   * If the input is an external source then an external dataset is created for tracking purpose and returned.
   * If the input is a regular dataset or a stream then it is already trackable, hence same input is returned.
   *
   * @param admin {@link Admin} used to create external dataset
   * @param input input to be tracked
   * @return an external dataset if input is an external source, otherwise the same input that is passed-in is returned
   */
  public static Input makeTrackable(Admin admin, Input input) {
    // If input is not an external source, return the same input as it can be tracked by itself.
    if (!(input instanceof Input.InputFormatProviderInput)) {
      return input;
    }

    // Input is an external source, create an external dataset so that it can be tracked.
    String inputName = input.getName();
    InputFormatProvider inputFormatProvider = ((Input.InputFormatProviderInput) input).getInputFormatProvider();
    Map<String, String> inputFormatConfiguration = inputFormatProvider.getInputFormatConfiguration();

    // Input is a dataset that implements input format provider,
    // this too can be tracked by itself without creating an external dataset
    if (inputFormatProvider instanceof Dataset) {
      return input;
    }

    try {
      // Create an external dataset for the input format for lineage tracking
      String datasetName = NAME_PREFIX + inputName;
      Map<String, String> arguments = new HashMap<>();
      arguments.put("input.format.class", inputFormatProvider.getInputFormatClassName());
      arguments.putAll(inputFormatConfiguration);
      if (!admin.datasetExists(datasetName)) {
        // Note: the dataset properties are the same as the arguments since we cannot identify them separately
        // since they are mixed up in a single configuration object (CDAP-5674)
        // Also, the properties of the external dataset created will contain runtime arguments for the same reason.
        admin.createDataset(datasetName, "externalDataset", DatasetProperties.builder().addAll(arguments).build());
      }
      return Input.ofDataset(datasetName, Collections.unmodifiableMap(arguments)).alias(input.getAlias());
    } catch (DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * If the output is an external sink then an external dataset is created for tracking purpose and returned.
   * If the output is a regular dataset then it is already trackable, hence same output is returned.
   *
   * @param admin {@link Admin} used to create external dataset
   * @param output output to be tracked
   * @return an external dataset if output is an external sink, otherwise the same output is returned
   */
  public static Output makeTrackable(Admin admin, Output output) {
    // If output is not an external sink, return the same output as it can be tracked by itself.
    if (!(output instanceof Output.OutputFormatProviderOutput)) {
      return output;
    }

    // Output is an external sink, create an external dataset so that it can be tracked.
    String outputName = output.getName();
    OutputFormatProvider outputFormatProvider = ((Output.OutputFormatProviderOutput) output).getOutputFormatProvider();
    Map<String, String> outputFormatConfiguration = outputFormatProvider.getOutputFormatConfiguration();

    // Output is a dataset that implements input format provider,
    // this can be tracked by itself without creating an external dataset
    if (outputFormatProvider instanceof Dataset) {
      return output;
    }

    // Output is an external sink, create an external dataset so that it can be tracked.
    try {
      // Create an external dataset for the output format for lineage tracking
      String datasetName = NAME_PREFIX + outputName;
      Map<String, String> arguments = new HashMap<>();
      arguments.put("output.format.class", outputFormatProvider.getOutputFormatClassName());
      arguments.putAll(outputFormatConfiguration);
      if (!admin.datasetExists(datasetName)) {
        // Note: the dataset properties are the same as the arguments since we cannot identify them separately
        // since they are mixed up in a single configuration object (CDAP-5674)
        // Also, the properties of the external dataset created will contain runtime arguments for the same reason.
        admin.createDataset(datasetName, "externalDataset", DatasetProperties.builder().addAll(arguments).build());
      }
      return Output.ofDataset(datasetName, Collections.unmodifiableMap(arguments)).alias(output.getAlias());
    } catch (DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  // To prevent instantiation
  private ExternalDatasets() {}
}
