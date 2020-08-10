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

package io.cdap.cdap.etl.common;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.etl.api.lineage.AccessType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * This class is used to create external datasets to track external sources/sinks.
 */
public final class ExternalDatasets {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalDatasets.class);
  private static final String EXTERNAL_DATASET_TYPE = "externalDataset";

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
      Map<String, String> arguments = new HashMap<>();
      arguments.put("input.format.class", inputFormatProvider.getInputFormatClassName());
      arguments.putAll(inputFormatConfiguration);
      if (!admin.datasetExists(inputName)) {
        // Note: the dataset properties are the same as the arguments since we cannot identify them separately
        // since they are mixed up in a single configuration object (CDAP-5674)
        // Also, the properties of the external dataset created will contain runtime arguments for the same reason.
        admin.createDataset(inputName, EXTERNAL_DATASET_TYPE, DatasetProperties.of(arguments));
      } else {
        // Check if the external dataset name clashes with an existing CDAP Dataset
        String datasetType = admin.getDatasetType(inputName);
        if (!EXTERNAL_DATASET_TYPE.equals(datasetType)) {
          throw new IllegalArgumentException(
            "An external source cannot have the same name as an existing CDAP Dataset instance " + inputName);
        }
      }
      return Input.ofDataset(inputName, Collections.unmodifiableMap(arguments)).alias(input.getAlias());
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
      Map<String, String> arguments = new HashMap<>();
      arguments.put("output.format.class", outputFormatProvider.getOutputFormatClassName());
      arguments.putAll(outputFormatConfiguration);
      if (!admin.datasetExists(outputName)) {
        // Note: the dataset properties are the same as the arguments since we cannot identify them separately
        // since they are mixed up in a single configuration object (CDAP-5674)
        // Also, the properties of the external dataset created will contain runtime arguments for the same reason.
        admin.createDataset(outputName, EXTERNAL_DATASET_TYPE, DatasetProperties.of(arguments));
      } else {
        // Check if the external dataset name clashes with an existing CDAP Dataset
        String datasetType = admin.getDatasetType(outputName);
        if (!EXTERNAL_DATASET_TYPE.equals(datasetType)) {
          throw new IllegalArgumentException(
            "An external sink cannot have the same name as an existing CDAP Dataset instance " + outputName);
        }
      }
      return Output.ofDataset(outputName, Collections.unmodifiableMap(arguments)).alias(output.getAlias());
    } catch (DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register lineage for this Spark program using the given reference name
   *
   * @param referenceName reference name used for source
   * @param accessType the access type of the lineage
   * @throws DatasetManagementException thrown if there was an error in creating reference dataset
   */
  public static void registerLineage(Admin admin, String referenceName,
                                     AccessType accessType, @Nullable Schema schema,
                                     Supplier<Dataset> datasetSupplier) throws DatasetManagementException {
    DatasetProperties datasetProperties;
    if (schema == null) {
      datasetProperties = DatasetProperties.EMPTY;
    } else {
      datasetProperties = DatasetProperties.of(Collections.singletonMap(DatasetProperties.SCHEMA, schema.toString()));
    }
    try {
      if (!admin.datasetExists(referenceName)) {
        admin.createDataset(referenceName, EXTERNAL_DATASET_TYPE, datasetProperties);
      }
    } catch (InstanceConflictException ex) {
      // Might happen if this is executed in parallel across multiple pipeline runs.
    }

    // we cannot instantiate ExternalDataset here - it is in CDAP data-fabric,
    // and this code (the pipeline app) cannot depend on that. Thus, use reflection
    // to invoke a method on the dataset.
    Dataset ds = datasetSupplier.get();
    Class<? extends Dataset> dsClass = ds.getClass();

    switch (accessType) {
      case READ:
        invokeMethod(referenceName, ds, dsClass, "recordRead", accessType);
        break;
      case WRITE:
        invokeMethod(referenceName, ds, dsClass, "recordWrite", accessType);
        break;
      default:
        LOG.warn("Failed to register lineage because of unknown access type {}", accessType);
    }
  }

  private static void invokeMethod(String referenceName, Dataset ds, Class<? extends Dataset> dsClass,
                                   String methodName, AccessType accessType) {
    try {
      Method method = dsClass.getMethod(methodName);
      method.invoke(ds);
    } catch (NoSuchMethodException e) {
      // should never happen unless somebody changes ExternalDataset in cdap-data-fabric
      LOG.warn("ExternalDataset '{}' does not have method '{}'. " +
                 "Can't register {} lineage for this dataset", referenceName, methodName, accessType);
    } catch (Exception e) {
      LOG.warn("Unable to register {} access for dataset {}", accessType, referenceName);
    }
  }

  // To prevent instantiation
  private ExternalDatasets() {}
}
