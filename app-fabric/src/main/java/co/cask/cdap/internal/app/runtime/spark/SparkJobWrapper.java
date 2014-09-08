/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.api.spark.JavaSparkJob;
import co.cask.cdap.api.spark.ScalaSparkJob;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Class which wraps around user's job class to integrate the spark program with CDAP.
 * This first command line argument to this class is the name of the user's Spark job class followed by the arguments
 * which will be passed to user's job class.
 * This Wrapper class is submitted to Spark and it does the following:
 * <ol>
 * <li>
 * Validates that there is at least {@link SparkJobWrapper#JOB_WRAPPER_ARGUMENTS_SIZE} command line arguments
 * </li>
 * <li>
 * Gets the user's job class through Spark's ExecutorURLClassLoader.
 * </li>
 * <li>
 * Sets {@link SparkContext} to concrete implementation of {@link JavaSparkContext} if user job implements {@link
 * JavaSparkJob} or to {@link ScalaSparkContext} if user's job implements {@link ScalaSparkJob}
 * </li>
 * <li>
 * Run user's job with extracted arguments from the argument list
 * </li>
 * </ol>
 */

public class SparkJobWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJobWrapper.class);
  private static final int JOB_WRAPPER_ARGUMENTS_SIZE = 1;
  private final String[] arguments;
  private final Class userJobClass;
  private static SparkContext sparkContext;
  private static boolean scalaJobFlag;

  /**
   * Constructor
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's job class is not found
   */
  public SparkJobWrapper(String[] args) {
    arguments = validateArgs(args);
    try {
      // get the Spark job main class with the custom classloader created by spark which has the program and
      // dependency jar.
      userJobClass = Class.forName(arguments[0], true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Unable to find the user job class: {}", arguments[0], cnfe);
      throw Throwables.propagate(cnfe);
    }
    setSparkContext();
  }

  public static void main(String[] args) {
    new SparkJobWrapper(args).instantiateUserJobClass();
  }

  /**
   * Validates command line arguments being passed
   * Expects at least {@link SparkJobWrapper#JOB_WRAPPER_ARGUMENTS_SIZE} command line arguments to be present
   *
   * @param arguments String[] the arguments
   * @return String[] if the command line arguments are sufficient else throws a {@link RuntimeException}
   * @throws IllegalArgumentException if the required numbers of command line arguments were not present
   */
  private String[] validateArgs(String[] arguments) {
    if (arguments.length < JOB_WRAPPER_ARGUMENTS_SIZE) {
      throw new IllegalArgumentException("Insufficient number of arguments. User's job class name followed by its " +
                                           "arguments (if any) should be provided");
    }
    return arguments;
  }

  /**
   * Extracts arguments belonging to the user's job class
   *
   * @return String[] of arguments with which user's job class should be called
   */
  private String[] extractUserArgs() {
    String[] userJobArgs = new String[(arguments.length - JOB_WRAPPER_ARGUMENTS_SIZE)];
    System.arraycopy(arguments, JOB_WRAPPER_ARGUMENTS_SIZE, userJobArgs, 0,
                     (arguments.length - JOB_WRAPPER_ARGUMENTS_SIZE));
    return userJobArgs;
  }

  /**
   * Instantiate an object of user's job class and call {@link #runUserJob(Object)} to run it
   *
   * @throws RuntimeException if failed to instantiate an object of user's job class
   */
  private void instantiateUserJobClass() {
    try {
      Object userJobObject = userJobClass.newInstance();
      runUserJob(userJobObject);
    } catch (InstantiationException ie) {
      LOG.warn("Unable to instantiate an object of user's job class: {}", arguments[0], ie);
      throw Throwables.propagate(ie);
    } catch (IllegalAccessException iae) {
      LOG.warn("Illegal access to class: {}", arguments[0] + "or to its constructor", iae);
      throw Throwables.propagate(iae);
    }
  }

  /**
   * Sets the {@link SparkContext} to {@link JavaSparkContext} or to {@link ScalaSparkContext} depending on whether
   * the user class implements {@link JavaSparkJob} or {@link ScalaSparkJob}
   */
  public void setSparkContext() {
    if (JavaSparkJob.class.isAssignableFrom(userJobClass)) {
      sparkContext = new JavaSparkContext();
    } else if (ScalaSparkJob.class.isAssignableFrom(userJobClass)) {
      sparkContext = new ScalaSparkContext();
      scalaJobFlag = true;
    } else {
      throw new IllegalArgumentException("User's Spark Job must implement either JavaSparkJob or ScalaSparkJob");
    }
  }

  /**
   * Extracts arguments which belongs to user's job and then invokes the run method on the user's job object with the
   * arguments and the appropriate implementation {@link SparkContext}
   *
   * @param userJobObject the user job's object
   * @throws RuntimeException if failed to invokeUserJob main function on the user's job object
   */
  private void runUserJob(Object userJobObject) {
    String[] userJobArgs = extractUserArgs();
    try {
      Method userJobMain = userJobClass.getMethod("run", String[].class, SparkContext.class);
      userJobMain.invoke(userJobObject, userJobArgs, sparkContext);
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find run method in user's job class: {}", userJobObject.getClass().getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access run method in user's job class: {}", userJobObject.getClass().getName(), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("User's job class run method threw an exception", ite);
      throw Throwables.propagate(ite);
    }
  }

  /**
   * @return {@link SparkContext}
   */
  public static SparkContext getSparkContext() {
    return sparkContext;
  }

  public static void stopJob() {
    if (scalaJobFlag) {
      ((org.apache.spark.SparkContext) getSparkContext().getOriginalSparkContext()).stop();
    } else {
      ((org.apache.spark.api.java.JavaSparkContext) getSparkContext().getOriginalSparkContext()).stop();
    }
  }
}
