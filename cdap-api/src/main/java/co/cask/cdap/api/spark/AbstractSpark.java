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

package co.cask.cdap.api.spark;

import java.util.Map;

/**
 * This abstract class provides a default implementation of {@link Spark} methods for easy extension.
 */
public abstract class AbstractSpark implements Spark {

  private SparkConfigurer configurer;

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
  protected final SparkConfigurer getConfigurer() {
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
   * Sets the job main class name in specification. The main method of this class will be called to run the
   * Spark job
   *
   * @param mainClass the class containing the main method
   */
  protected final void setMainClass(Class<?> mainClass) {
    setMainClassName(mainClass.getName());
  }

  /**
   * Sets the Spark job main class name in specification. The main method of this class will be called to run the
   * Spark job
   *
   * @param mainClassName the fully qualified name of class containing the main method
   */
  protected final void setMainClassName(String mainClassName) {
    configurer.setMainClassName(mainClassName);
  }

  /**
   * Sets a set of properties that will be available through the {@link SparkSpecification#getProperties()}
   * at runtime.
   *
   * @param properties the properties to set
   */
  protected final void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  @Override
  public void beforeSubmit(SparkContext context) throws Exception {
    // Do nothing by default
  }

  @Override
  public void onFinish(boolean succeeded, SparkContext context) throws Exception {
    // Do nothing by default
  }
}
