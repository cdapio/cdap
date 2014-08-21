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

import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkContextFactory;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.runtime.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which gives a default implementation for {@link SparkContextFactory} and provide
 * a custom implementation of SparkContext depending upon the argument passed by the user.
 * <p>
 * <ul>
 * <li>
 * {@link JavaSparkContext}: if the argument passed by user is an instance of
 * {@link org.apache.spark.api.java.JavaSparkContext}
 * </li>
 * <li>
 * {@link ScalaSparkContext}: if the argument passed by the user is an instance of
 * {@link org.apache.spark.SparkContext}
 * </li>
 * </ul>
 * </p>
 */
public class DefaultSparkContextFactory implements SparkContextFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSparkContextFactory.class);

  private final long logicalStartTime;
  private final SparkSpecification spec;
  private final Arguments runtimeArguments;

  //TODO: This constructor should be removed once we have ability to create this object from inside CDAP with the
  // arguments needed by the parametrized constructor
  public DefaultSparkContextFactory() {
    logicalStartTime = 0;
    spec = null;
    runtimeArguments = null;
  }

  public DefaultSparkContextFactory(long logicalStartTime, SparkSpecification spec, Arguments runtimeArguments) {
    this.logicalStartTime = logicalStartTime;
    this.spec = spec;
    this.runtimeArguments = runtimeArguments;
  }

  /**
   * Returns the appropriate CDAP custom {@link SparkContext}
   *
   * @param context the Apache Spark context which can be either SparkContext (for scala job) or JavaSparkContext
   *                (for Java jobs)
   * @return The appropriate {@link SparkContext} which an instance of:
   * <ol>
   * <li> {@link JavaSparkContext}: If the parameter provided is an
   * instance of {@link org.apache.spark.api.java.JavaSparkContext}</li>
   * <li> {@link ScalaSparkContext}: If the parameter provided is an instance of
   * {@link org.apache.spark.SparkContext}</li>
   * </ol>
   */
  @Override
  public <T> SparkContext create(T context) {
    if (context instanceof org.apache.spark.api.java.JavaSparkContext) {
      return new JavaSparkContext((org.apache.spark.api.java.JavaSparkContext) context, logicalStartTime,
                                  spec, runtimeArguments);
    } else if (context instanceof org.apache.spark.SparkContext) {
      return new ScalaSparkContext((org.apache.spark.SparkContext) context,
                                   logicalStartTime, spec, runtimeArguments);
    } else {
      LOG.warn("The passed context should be of type Apache Spark's SparkContext or JavaSparkContext");
      throw new IllegalArgumentException("Invalid context type. Context type must be either JavaSparkContext or " +
                                           "SparkContext from Apache Spark");
    }
  }
}
