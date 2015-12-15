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

package co.cask.cdap.etl.batch.spark;

import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Handles writes to batch sinks. Maintains a mapping from sinks to their outputs and handles serialization and
 * deserialization for those mappings.
 */
final class SparkBatchSinkFactory {

  static SparkBatchSinkFactory deserialize(InputStream inputStream) throws IOException {
    DataInput input = new DataInputStream(inputStream);
    Map<String, OutputFormatProvider> outputFormatProviders = Serializations.deserializeMap(
      input, new Serializations.ObjectReader<OutputFormatProvider>() {
      @Override
      public OutputFormatProvider read(DataInput input) throws IOException {
        return new BasicOutputFormatProvider(input.readUTF(),
                                             Serializations.deserializeMap(input,
                                                                           Serializations.createStringObjectReader()));
      }
    });
    Map<String, DatasetInfo> datasetInfos = Serializations.deserializeMap(
      input, new Serializations.ObjectReader<DatasetInfo>() {
      @Override
      public DatasetInfo read(DataInput input) throws IOException {
        return DatasetInfo.deserialize(input);
      }
    });
    Map<String, Set<String>> sinkOutputs = Serializations.deserializeMap(
      input, Serializations.createStringSetObjectReader());
    return new SparkBatchSinkFactory(outputFormatProviders, datasetInfos, sinkOutputs);
  }

  private final Map<String, OutputFormatProvider> outputFormatProviders;
  private final Map<String, DatasetInfo> datasetInfos;
  private final Map<String, Set<String>> sinkOutputs;

  SparkBatchSinkFactory() {
    this.outputFormatProviders = new HashMap<>();
    this.datasetInfos = new HashMap<>();
    this.sinkOutputs = new HashMap<>();
  }

  private SparkBatchSinkFactory(Map<String, OutputFormatProvider> providers,
                                Map<String, DatasetInfo> datasetInfos,
                                Map<String, Set<String>> sinkOutputs) {
    this.outputFormatProviders = providers;
    this.datasetInfos = datasetInfos;
    this.sinkOutputs = sinkOutputs;
  }

  void addOutput(String stageName, String outputName, OutputFormatProvider outputFormatProvider) {
    if (outputFormatProviders.containsKey(outputName) || datasetInfos.containsKey(outputName)) {
      throw new IllegalArgumentException("OutputFormatProvider with name " + outputName + " is already defined. " +
                                           "Cannot add multiple OutputFormatProviders with the same name.");
    }
    outputFormatProviders.put(outputName, outputFormatProvider);
    addStageOutput(stageName, outputName);
  }

  void addOutput(String stageName, String datasetName, Map<String, String> datasetArgs) {
    if (outputFormatProviders.containsKey(datasetName) || datasetInfos.containsKey(datasetName)) {
      throw new IllegalArgumentException("Output dataset with name " + datasetName + " is already defined. " +
                                           "Cannot add the same dataset as output multiple times.");
    }
    datasetInfos.put(datasetName, new DatasetInfo(datasetName, datasetArgs, null));
    addStageOutput(stageName, datasetName);
  }

  void serialize(OutputStream outputStream) throws IOException {
    DataOutput output = new DataOutputStream(outputStream);
    Serializations.serializeMap(outputFormatProviders, new Serializations.ObjectWriter<OutputFormatProvider>() {
      @Override
      public void write(OutputFormatProvider outputFormatProvider, DataOutput output) throws IOException {
        output.writeUTF(outputFormatProvider.getOutputFormatClassName());
        Serializations.serializeMap(outputFormatProvider.getOutputFormatConfiguration(),
                                    Serializations.createStringObjectWriter(), output);
      }
    }, output);
    Serializations.serializeMap(datasetInfos, new Serializations.ObjectWriter<DatasetInfo>() {
      @Override
      public void write(DatasetInfo datasetInfo, DataOutput output) throws IOException {
        datasetInfo.serialize(output);
      }
    }, output);
    Serializations.serializeMap(sinkOutputs, Serializations.createStringSetObjectWriter(), output);
  }

  <K, V> void writeFromRDD(JavaPairRDD<K, V> rdd, SparkContext sparkContext, String sinkName,
                           Class<K> keyClass, Class<V> valueClass) {
    Set<String> outputNames = sinkOutputs.get(sinkName);
    if (outputNames == null || outputNames.size() == 0) {
      // should never happen if validation happened correctly at pipeline configure time
      throw new IllegalArgumentException(sinkName + " has no outputs. " +
                                           "Please check that the sink calls addOutput at some point.");
    }

    for (String outputName : outputNames) {
      OutputFormatProvider outputFormatProvider = outputFormatProviders.get(outputName);
      if (outputFormatProvider != null) {
        Configuration hConf = new Configuration();
        hConf.clear();
        for (Map.Entry<String, String> entry : outputFormatProvider.getOutputFormatConfiguration().entrySet()) {
          hConf.set(entry.getKey(), entry.getValue());
        }
        hConf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatProvider.getOutputFormatClassName());
        rdd.saveAsNewAPIHadoopDataset(hConf);
      }

      DatasetInfo datasetInfo = datasetInfos.get(outputName);
      if (datasetInfo != null) {
        sparkContext.writeToDataset(rdd, datasetInfo.getDatasetName(),
                                    keyClass, valueClass, datasetInfo.getDatasetArgs());
      }
    }
  }

  private void addStageOutput(String stageName, String outputName) {
    Set<String> outputs = sinkOutputs.get(stageName);
    if (outputs == null) {
      outputs = new HashSet<>();
    }
    outputs.add(outputName);
    sinkOutputs.put(stageName, outputs);
  }

  private static final class BasicOutputFormatProvider implements OutputFormatProvider {

    private final String outputFormatClassName;
    private final Map<String, String> configuration;

    private BasicOutputFormatProvider(String outputFormatClassName, Map<String, String> configuration) {
      this.outputFormatClassName = outputFormatClassName;
      this.configuration = ImmutableMap.copyOf(configuration);
    }

    @Override
    public String getOutputFormatClassName() {
      return outputFormatClassName;
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return configuration;
    }
  }
}
