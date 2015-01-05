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

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetOutputFormat;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private static final String SPARK_METRICS_CONF_KEY = "spark.metrics.conf";

  private final Configuration hConf;
  private final long logicalStartTime;
  private final SparkSpecification spec;
  private final Map<String, String> runtimeArguments;
  private final BasicSparkContext basicSparkContext;
  private final SparkConf sparkConf;

  public AbstractSparkContext(BasicSparkContext basicSparkContext) {
    hConf = loadHConf();
    this.basicSparkContext = basicSparkContext;
    this.logicalStartTime = basicSparkContext.getLogicalStartTime();
    this.spec = basicSparkContext.getSpecification();
    this.runtimeArguments = basicSparkContext.getRuntimeArguments();
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
    sparkConf.set(SPARK_METRICS_CONF_KEY, basicSparkContext.getMetricsPropertyFile().getAbsolutePath());
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
   * @throws IllegalArgumentException if the {@link Dataset} to read is not {@link BatchReadable}
   */
  Configuration setInputDataset(String datasetName) {
    Configuration hConf = new Configuration(getHConf());
    Dataset dataset = basicSparkContext.getDataset(datasetName);
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
   * Gets a {@link Stream} as a {@link JavaPairRDD} for Java program and {@link NewHadoopRDD} for Scala Program
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @return the RDD created from the {@link Stream} to be read
   */
  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass) {
    return readFromStream(streamName, vClass, 0, System.currentTimeMillis(), null);
  }

  /**
   * Gets a {@link Stream} as a {@link JavaPairRDD} for Java program and {@link NewHadoopRDD} for Scala Program
   *
   * @param streamName the name of the {@link Stream} to be read as an RDD
   * @param vClass     the value class
   * @param startTime  the starting time of the stream to be read
   * @param endTime    the ending time of the streams to be read
   * @return the RDD created from the {@link Stream} to be read
   */
  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime) {
    return readFromStream(streamName, vClass, startTime, endTime, null);
  }

  /**
   * Sets the input to a {@link Stream}
   *
   * @param stream the stream to which input will be set to
   * @param vClass the value class which can be either {@link Text} or {@link BytesWritable}
   * @return updated {@link Configuration}
   * @throws IOException if the given {@link Stream} is not found or the {@link StreamEventDecoder} was not identified
   */
  Configuration setStreamInputDataset(StreamBatchReadable stream, Class<?> vClass) throws IOException {
    Configuration hConf = new Configuration(getHConf());
    configureStreamInput(hConf, stream, vClass);
    return hConf;
  }

  /**
   * Adds the needed information to read from the given {@link Stream} in the {@link Configuration}
   *
   * @param hConf  the {@link Configuration} to which the stream info will be added
   * @param stream a {@link StreamBatchReadable} for the given stream
   * @param vClass the value class which can be either {@link Text} or {@link BytesWritable}
   * @throws IOException
   */
  private void configureStreamInput(Configuration hConf, StreamBatchReadable stream, Class<?> vClass)
    throws IOException {
    StreamConfig streamConfig = basicSparkContext.getStreamAdmin().getConfig(stream.getStreamName());
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    StreamInputFormat.setTTL(hConf, streamConfig.getTTL());
    StreamInputFormat.setStreamPath(hConf, streamPath.toURI());
    StreamInputFormat.setTimeRange(hConf, stream.getStartTime(), stream.getEndTime());

    String decoderType = stream.getDecoderType();
    if (decoderType == null) {
      // If the user don't specify the decoder, detect the type
      StreamInputFormat.inferDecoderClass(hConf, vClass);
    } else {
      StreamInputFormat.setDecoderClassName(hConf, decoderType);
    }
    hConf.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, StreamInputFormat.class, InputFormat.class);

    LOG.info("Using Stream as input from {}", stream.toURI());
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
    if (runtimeArguments.containsKey(argsKey)) {
      return SPACES.split(runtimeArguments.get(argsKey).trim());
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
    return runtimeArguments;
  }

  @Override
  public ServiceDiscoverer getServiceDiscoverer() {
    return basicSparkContext.getSerializableServiceDiscoverer();
  }

  @Override
  public Map<String, String> getState() {
    return basicSparkContext.getState();
  }

  @Override
  public void saveState(Map<String, String> state) {
    basicSparkContext.saveState(state);
  }

  public Metrics getMetrics() {
    return basicSparkContext.getMetrics();
  }
}
