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

import co.cask.cdap.api.spark.SparkContextFactory;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
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
 * Extract user job arguments by removing the first argument from the argument list
 * </li>
 * <li>
 * Creates a new instance of user's job class
 * </li>
 * <li>
 * Binds the {@link SparkContextFactory} field to a concrete implementation {@link DefaultSparkContextFactory}
 * depending upon whether user's job class is written in Java or Scala
 * </li>
 * <li>
 * Calls the user's job main class
 * </li>
 * </ol>
 */
//TODO: This class should be made private when we will have SparkProgramRunner using it and not using it from outside.
public class SparkJobWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJobWrapper.class);
  private static final int JOB_WRAPPER_ARGUMENTS_SIZE = 1;
  private static final String SCALA_SETTER_SUFFIX = "_$eq";

  private final String[] arguments;
  private final Class userJobClass;
  private final Field sparkContextFactoryField;

  /**
   * Constructor
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's job class is not found
   */
  public SparkJobWrapper(String[] args) {
    arguments = validateArgs(args);
    try {
      userJobClass = Class.forName(arguments[0]);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Unable to find the user job class: {}", arguments[0], cnfe);
      throw Throwables.propagate(cnfe);
    }
    sparkContextFactoryField = getSparkContxtFactoryField();
  }

  public static void main(String[] args) {
    new SparkJobWrapper(args).invoke();
  }

  /**
   * Determines whether the user job is a scala job or not through {@link SparkJobWrapper#isScalaJob()} and then
   * calls {@link SparkJobWrapper#invokeUserJobMain(Object)} accordingly.
   */
  private void invoke() {
    if (isScalaJob()) {
      Object userJobObject = scalaJobSparkContextBinder();
      invokeUserJobMain(userJobObject);
    } else {
      javaJobSparkContextBinder();
      invokeUserJobMain(null);
    }
  }

  /**
   * Sets {@link SparkJobWrapper#sparkContextFactoryField} to the {@link SparkContextFactory} field in user's job class
   *
   * @return {@link SparkContextFactory} {@link Field}
   * @throws RuntimeException if the {@link SparkContextFactory} field is not found in user's job class
   */
  private Field getSparkContxtFactoryField() {
    for (Field field : userJobClass.getDeclaredFields()) {
      if (SparkContextFactory.class.isAssignableFrom(field.getType())) {
        return field;
      }
    }
    throw new RuntimeException("SparkContextFactory field not found in user's job class. Please include a member " +
                                 "field of this class");
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
   * Instantiate an object of user's job class
   *
   * @return a new object of user's job class
   * @throws RuntimeException if failed to instantiate an object of user's job class
   */
  private Object instantiateUserJobClass() {
    Object userJobObject;
    try {
      userJobObject = userJobClass.newInstance();
    } catch (InstantiationException ie) {
      LOG.warn("Unable to instantiate an object of user's job class: {}", arguments[0], ie);
      throw Throwables.propagate(ie);
    } catch (IllegalAccessException iae) {
      LOG.warn("Illegal access to class: {}", arguments[0] + "or to its constructor", iae);
      throw Throwables.propagate(iae);
    }
    return userJobObject;
  }

  /**
   * Injects {@link DefaultSparkContextFactory} to {@link SparkContextFactory} if the user's job class
   * is written in Java
   *
   * @throws RuntimeException if the binding to {@link DefaultSparkContextFactory} fails
   */
  private void javaJobSparkContextBinder() {
    if (!sparkContextFactoryField.isAccessible()) {
      sparkContextFactoryField.setAccessible(true);
    }
    try {
      sparkContextFactoryField.set(null, new DefaultSparkContextFactory());
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access field: {}", sparkContextFactoryField.getName(), iae);
      throw Throwables.propagate(iae);
    }
  }

  /**
   * Injects {@link DefaultSparkContextFactory} to {@link SparkContextFactory} if the user's job class
   * is written in Scala
   *
   * @throws RuntimeException if the binding to {@link DefaultSparkContextFactory} fails
   */
  private Object scalaJobSparkContextBinder() {
    Object userJobObject = instantiateUserJobClass();
    try {
      Method setSparkContext = userJobObject.getClass().getMethod(sparkContextFactoryField.getName() +
                                                                    SCALA_SETTER_SUFFIX, SparkContextFactory.class);
      setSparkContext.invoke(userJobObject, new DefaultSparkContextFactory());
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find setter method for field: {}", sparkContextFactoryField.getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access method: {}", (sparkContextFactoryField.getName() + SCALA_SETTER_SUFFIX), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("The method: {}", (sparkContextFactoryField.getName() + SCALA_SETTER_SUFFIX) + "threw an exception",
               ite);
      throw Throwables.propagate(ite);
    }
    return userJobObject;
  }

  /**
   * Determines if the user's job is written in Scala or Java
   * We assume that if we find a setter method for {@link SparkContextFactory} with method name "fieldname_$eq" then
   * the user's job is in Scala or else in Java.
   *
   * @return a boolean which is true if the job is in scala
   */
  private boolean isScalaJob() {
    Method[] methods = userJobClass.getMethods();
    for (Method curMethod : methods) {
      if (curMethod.getName().equalsIgnoreCase((sparkContextFactoryField.getName() + SCALA_SETTER_SUFFIX))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Extracts arguments which belongs to user's job and then invokes the main function on the user's job object
   * passing the arguments
   *
   * @param userJobObject the user job's object
   * @throws RuntimeException if failed to invoke main function on the user's job object
   */
  private void invokeUserJobMain(Object userJobObject) {
    String[] userJobArgs = extractUserArgs();
    try {
      Method userJobMain = userJobClass.getMethod("main", String[].class);
      userJobMain.invoke(userJobObject, (Object) userJobArgs);
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find main method in user's job class: {}", userJobObject.getClass().getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access main method in user's job class: {}", userJobObject.getClass().getName(), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("User's job class main method threw an exception", ite);
      throw Throwables.propagate(ite);
    }
  }
}
