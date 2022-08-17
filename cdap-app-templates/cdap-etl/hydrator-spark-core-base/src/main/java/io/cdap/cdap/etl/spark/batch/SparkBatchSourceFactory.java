/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import com.google.common.base.Objects;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.Split;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineInput;
import io.cdap.cdap.etl.batch.BasicInputFormatProvider;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Thread.currentThread;

/**
 * A POJO class for storing source information being set from {@link SparkBatchSourceContext} and used in
 * {@link BatchSparkPipelineDriver}.
 */
public final class SparkBatchSourceFactory {

  private final Map<String, InputFormatProvider> inputFormatProviders;
  private final Map<String, DatasetInfo> datasetInfos;
  private final Map<String, Set<String>> sourceInputs;
  private final Map<String, SQLEngineInput> sqlInputs;

  public SparkBatchSourceFactory() {
    this.inputFormatProviders = new HashMap<>();
    this.datasetInfos = new HashMap<>();
    this.sourceInputs = new HashMap<>();
    this.sqlInputs = new HashMap<>();
  }

  public void addInput(String stageName, Input input) {
    if (input instanceof Input.DatasetInput) {
      // Note if input format provider is trackable then it comes in as DatasetInput
      Input.DatasetInput datasetInput = (Input.DatasetInput) input;
      addInput(stageName, datasetInput.getName(), datasetInput.getAlias(), datasetInput.getArguments(),
               datasetInput.getSplits());
    } else if (input instanceof Input.InputFormatProviderInput) {
      Input.InputFormatProviderInput ifpInput = (Input.InputFormatProviderInput) input;
      addInput(stageName, ifpInput.getAlias(),
               new BasicInputFormatProvider(ifpInput.getInputFormatProvider().getInputFormatClassName(),
                                            ifpInput.getInputFormatProvider().getInputFormatConfiguration()));
    } else if (input instanceof SQLEngineInput) {
      addInput(stageName, input.getAlias(), (SQLEngineInput) input);
    }
  }

  /**
   * Add Dataset Input
   * @param stageName stage name
   * @param datasetName dataset name
   * @param alias stage alias
   * @param datasetArgs dataset arguments
   * @param splits list of splits
   */
  private void addInput(String stageName, String datasetName, String alias, Map<String, String> datasetArgs,
                        List<Split> splits) {
    duplicateAliasCheck(alias);
    datasetInfos.put(alias, new DatasetInfo(datasetName, datasetArgs, splits));
    addStageInput(stageName, alias);
  }

  /**
   * Add InputFormatProvider Input
   * @param stageName stage name
   * @param alias stage alias
   * @param inputFormatProvider input format provider instance
   */
  private void addInput(String stageName, String alias, InputFormatProvider inputFormatProvider) {
    duplicateAliasCheck(alias);
    inputFormatProviders.put(alias, inputFormatProvider);
    addStageInput(stageName, alias);
  }

  /**
   * Add SQL Engine Input
   * @param stageName stage name
   * @param alias stage alias
   * @param sqlEngineInput input
   */
  private void addInput(String stageName, String alias,
                         SQLEngineInput sqlEngineInput) {
    if (sqlInputs.containsKey(stageName)) {
      throw new IllegalArgumentException("Input already configured: " + alias);
    }
    sqlInputs.put(alias, sqlEngineInput);
    addStageInput(stageName, alias);
  }

  private void duplicateAliasCheck(String alias) {
    if (inputFormatProviders.containsKey(alias) || datasetInfos.containsKey(alias)) {
      // this will never happen since alias will be unique since we append it with UUID
      throw new IllegalStateException(alias + " has already been added. Can't add an input with the same alias.");
    }
  }

  public SQLEngineInput getSQLEngineInput(String stageName) {
    return !sourceInputs.containsKey(stageName) ? null :
      sourceInputs.get(stageName)
        .stream()
        .map(sqlInputs::get)
        .filter(java.util.Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  public <K, V> JavaPairRDD<K, V> createRDD(JavaSparkExecutionContext sec, JavaSparkContext jsc, String sourceName,
                                            Class<K> keyClass, Class<V> valueClass) {
    Set<String> inputNames = sourceInputs.get(sourceName);

    // Exclude SQL inputs
    inputNames = inputNames.stream().filter(in -> !sqlInputs.containsKey(in)).collect(Collectors.toSet());

    if (inputNames == null || inputNames.isEmpty()) {
      // should never happen if validation happened correctly at pipeline configure time
      throw new IllegalArgumentException(
        sourceName + " has no input. Please check that the source calls setInput at some input.");
    }

    Iterator<String> inputsIter = inputNames.iterator();
    JavaPairRDD<K, V> inputRDD = createInputRDD(sec, jsc, inputsIter.next(), keyClass, valueClass);
    while (inputsIter.hasNext()) {
      inputRDD = inputRDD.union(createInputRDD(sec, jsc, inputsIter.next(), keyClass, valueClass));
    }
    return inputRDD;
  }

  @SuppressWarnings("unchecked")
  private <K, V> JavaPairRDD<K, V> createInputRDD(JavaSparkExecutionContext sec, JavaSparkContext jsc, String inputName,
                                                  Class<K> keyClass, Class<V> valueClass) {
    if (inputFormatProviders.containsKey(inputName)) {
      InputFormatProvider inputFormatProvider = inputFormatProviders.get(inputName);

      ClassLoader classLoader = Objects.firstNonNull(currentThread().getContextClassLoader(),
                                                     getClass().getClassLoader());

      return RDDUtils.readUsingInputFormat(jsc, inputFormatProvider, classLoader, keyClass,
                                           valueClass);
    }

    if (datasetInfos.containsKey(inputName)) {
      DatasetInfo datasetInfo = datasetInfos.get(inputName);
      return sec.fromDataset(datasetInfo.getDatasetName(), datasetInfo.getDatasetArgs());
    }
    // This should never happen since the constructor is private and it only get calls from static create() methods
    // which make sure one and only one of those source type will be specified.
    throw new IllegalStateException("Unknown source type");
  }

  private void addStageInput(String stageName, String inputName) {
    Set<String> inputs = sourceInputs.get(stageName);
    if (inputs == null) {
      inputs = new HashSet<>();
    }
    inputs.add(inputName);
    sourceInputs.put(stageName, inputs);
  }

}
