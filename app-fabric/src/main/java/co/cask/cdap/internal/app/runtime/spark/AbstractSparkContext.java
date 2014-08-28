/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputFormat;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;

/**
 * An abstract class which implements {@link SparkContext} and provide a concrete implementation for the common
 * functionality between {@link JavaSparkContext} and {@link ScalaSparkContext}
 */
abstract class AbstractSparkContext implements SparkContext {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkContext.class);

  private final Configuration hConf;
  private final long logicalStartTime;
  private final SparkSpecification spec;
  private final Arguments runtimeArguments;

  public AbstractSparkContext(long logicalStartTime, SparkSpecification spec, Arguments runtimeArguments) {
    this.logicalStartTime = logicalStartTime;
    this.spec = spec;
    this.runtimeArguments = runtimeArguments;
    this.hConf = loadHConf();
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
    Configuration hConf = new Configuration();
    hConf.clear();
    //TODO: The filename should be static final in the SparkRunner. Change this static string to that.
    URL url = getClass().getResource("/hConf.xml");
    if (url == null) {
      LOG.error("Unable to find Hadoop Configuration file in the submitted jar.");
      throw new RuntimeException("Hadoop Configuration file not found in the supplied jar. Please include Hadoop " +
                                   "Configuration file with name \"hConf.xml\"");
    }
    hConf.addResource(url);
    return hConf;
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

  @Override
  public ServiceDiscovered discover(String applicationId, String serviceId, String serviceName) {
    //TODO: Change this once we start supporting services in Spark.
    throw new UnsupportedOperationException("Service Discovery not supported");
  }
}
