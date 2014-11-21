/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An abstract class which implements {@link SparkContext} and provide a concrete implementation for the common
 * functionality between {@link JavaSparkContext} and {@link ScalaSparkContext}
 */
abstract class AbstractSparkContext implements SparkContext {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkContext.class);
  private static final Pattern SPACES = Pattern.compile("\\s+");
  private static final String[] NO_ARGS = {};

  private final Configuration hConf;
  private final long logicalStartTime;
  private final SparkSpecification spec;
  private final Arguments runtimeArguments;
  final BasicSparkContext basicSparkContext;
  private final SparkConf sparkConf;

  public AbstractSparkContext() {
    hConf = loadHConf();
    // Create an instance of BasicSparkContext from the Hadoop Configuration file which was just loaded
    SparkContextProvider sparkContextProvider = new SparkContextProvider(hConf);
    basicSparkContext = sparkContextProvider.get();
    this.logicalStartTime = basicSparkContext.getLogicalStartTime();
    this.spec = basicSparkContext.getSpecification();
    this.runtimeArguments = basicSparkContext.getRuntimeArgs();
    this.sparkConf = initializeSparkConf();
  }

  /**
   * Initializes the {@link SparkConf} with proper settings.
   *
   * @return the initialized {@link SparkConf}
   */
  private SparkConf initializeSparkConf() {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName(basicSparkContext.getProgramName());
    return sparkConf;
  }

  public SparkConf getSparkConf() {
    return sparkConf;
  }

  Configuration getHConf() {
    return hConf;
  }

  /**
   * Adds the supplied {@link Configuration} file as an resource
   * This configuration is needed to read/write {@link Dataset} using {@link DataSetInputFormat}/{@link
   * DataSetOutputFormat} by {@link JavaSparkContext#readFromDataset(String, Class, Class)} or
   * {@link ScalaSparkContext#readFromDataset(String, Class, Class)}
   * This function requires that the hConf.xml file containing {@link Configuration} is present in the job jar.
   */
  private Configuration loadHConf() {
    // TODO: Inject through Guice in Distributed mode, see CDAP-3
    Configuration hConf = new Configuration();
    hConf.clear();

    URL url = Thread.currentThread().getContextClassLoader().getResource(SparkRuntimeService.SPARK_HCONF_FILENAME);
    if (url == null) {
      LOG.error("Unable to find Hadoop Configuration file {} in the submitted jar.",
                SparkRuntimeService.SPARK_HCONF_FILENAME);
      throw new RuntimeException("Hadoop Configuration file not found in the supplied jar. Please include Hadoop " +
                                   "Configuration file with name " + SparkRuntimeService.SPARK_HCONF_FILENAME);
    }
    hConf.addResource(url);
    return hConf;
  }

  /**
   * Sets the input {@link Dataset} with splits in the {@link Configuration}
   *
   * @param datasetName the name of the {@link Dataset} to read from
   * @return updated {@link Configuration}
   * @throws {@link IllegalArgumentException} if the {@link Dataset} to read is not {@link BatchReadable}
   */
  Configuration setInputDataset(String datasetName) {
    Configuration hConf = new Configuration(getHConf());
    Dataset dataset = basicSparkContext.getDataSet(datasetName);
    List<Split> inputSplits;
    if (dataset instanceof BatchReadable) {
      BatchReadable curDataset = (BatchReadable) dataset;
      inputSplits = curDataset.getSplits();
    } else {
      throw new IllegalArgumentException("Failed to read dataset " + datasetName + ". The dataset does not implement" +
                                           " BatchReadable");
    }
    hConf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, SparkDatasetInputFormat.class, InputFormat.class);
    hConf.set(SparkDatasetInputFormat.HCONF_ATTR_INPUT_DATASET, datasetName);
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLIT_CLASS, inputSplits.get(0).getClass().getName());
    hConf.set(SparkContextConfig.HCONF_ATTR_INPUT_SPLITS, new Gson().toJson(inputSplits));
    return hConf;
  }

  /**
   * Sets the output {@link Dataset} with splits in the {@link Configuration}
   *
   * @param datasetName the name of the {@link Dataset} to write to
   * @return updated {@link Configuration}
   */
  Configuration setOutputDataset(String datasetName) {
    Configuration hConf = new Configuration(getHConf());
    hConf.set(SparkDatasetOutputFormat.HCONF_ATTR_OUTPUT_DATASET, datasetName);
    hConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SparkDatasetOutputFormat.class, OutputFormat.class);
    return hConf;
  }

  /**
   * Returns value of the given argument key as a String[]
   *
   * @param argsKey {@link String} which is the key for the argument
   * @return String[] containing all the arguments which is indexed by their position as they were supplied
   */
  @Override
  public String[] getRuntimeArguments(String argsKey) {
    if (runtimeArguments.hasOption(argsKey)) {
      return SPACES.split(runtimeArguments.getOption(argsKey).trim());
    } else {
      LOG.warn("Argument with key {} not found in Runtime Arguments", argsKey);
      return NO_ARGS;
    }
  }

  @Override
  public SparkSpecification getSpecification() {
    return spec;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    for (Map.Entry<String, String> runtimeArgument : runtimeArguments) {
      arguments.put(runtimeArgument);
    }
    return arguments.build();
  }

}
