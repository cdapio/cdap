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

package io.cdap.cdap.api.spark;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.internal.api.AbstractPluginConfigurable;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class provides a default implementation of {@link Spark} methods for easy
 * extension.
 */
@Beta
public abstract class AbstractSpark extends AbstractPluginConfigurable<SparkConfigurer>
    implements Spark, ProgramLifecycle<SparkClientContext> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSpark.class);
  private SparkConfigurer configurer;
  private SparkClientContext context;

  @Override
  public final void configure(SparkConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Override this method to configure this {@link Spark} job.
   */
  protected void configure() {
    // Default no-op
  }

  /**
   * Returns the {@link SparkConfigurer}, only available at configuration time.
   */
  @Override
  protected SparkConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link Spark}.
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link Spark}.
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets the job main class name in specification. The main method of this class will be called to
   * run the Spark job
   *
   * @param mainClass the class containing the main method
   */
  protected final void setMainClass(Class<?> mainClass) {
    setMainClassName(mainClass.getName());
  }

  /**
   * Sets the Spark job main class name in specification. The main method of this class will be
   * called to run the Spark job
   *
   * @param mainClassName the fully qualified name of class containing the main method
   */
  protected final void setMainClassName(String mainClassName) {
    configurer.setMainClassName(mainClassName);
  }

  /**
   * Sets a set of properties that will be available through the {@link
   * SparkSpecification#getProperties()} at runtime.
   *
   * @param properties the properties to set
   */
  protected final void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Sets the resources requirement for the Spark client process.
   */
  protected final void setClientResources(Resources resources) {
    configurer.setClientResources(resources);
  }

  /**
   * Sets the resources requirement for the Spark driver process.
   */
  protected final void setDriverResources(Resources resources) {
    configurer.setDriverResources(resources);
  }

  /**
   * Sets the resources requirement for the Spark executor processes.
   */
  protected final void setExecutorResources(Resources resources) {
    configurer.setExecutorResources(resources);
  }

  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public final void initialize(SparkClientContext context) throws Exception {
    this.context = context;
    initialize();
  }

  /**
   * Classes derived from {@link AbstractSpark} can override this method to initialize the {@link
   * Spark}. {@link SparkClientContext} will be available in this method using {@link
   * AbstractSpark#getContext}.
   *
   * @throws Exception if there is any error in initializing the Spark
   */
  @TransactionPolicy(TransactionControl.IMPLICIT)
  protected void initialize() throws Exception {
    // do nothing by default
  }


  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public void destroy() {
    // do nothing by default
  }

  /**
   * Return an instance of the {@link SparkClientContext}.
   */
  protected final SparkClientContext getContext() {
    return context;
  }
}
