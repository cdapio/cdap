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
 * Validates that there is at least {@link SparkJobWrapper#TOTAL_JOB_WRAPPER_ARGUMENTS} command line arguments
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
  private static final int TOTAL_JOB_WRAPPER_ARGUMENTS = 1;

  public static void main(String[] args) {
    SparkJobWrapper jobWrapper = new SparkJobWrapper();
    jobWrapper.validateArgs(args);
    String[] userJobArgs = jobWrapper.extractUserArgs(args);

    Object userJobObject = jobWrapper.sparkContextFactoryBinder(jobWrapper.instantiateUserJobClass(args[0]));
    jobWrapper.invokeUserJobMain(userJobObject, userJobArgs);
  }

  /**
   * Function to validate command line arguments being passed
   * Expects at least {@link SparkJobWrapper#TOTAL_JOB_WRAPPER_ARGUMENTS} command line arguments to be present
   *
   * @param arguments String[] of command line arguments
   * @throws IllegalArgumentException if the required numbers of command line arguments were not present
   */
  void validateArgs(String[] arguments) {
    if (arguments.length < TOTAL_JOB_WRAPPER_ARGUMENTS) {
      throw new IllegalArgumentException("Insufficient number of arguments. User's job class name followed by its " +
                                           "arguments (if any) should be provided");
    }
  }

  /**
   * Function to extract arguments belonging to the user's job class
   *
   * @param arguments command line arguments provided to {@link SparkJobWrapper}
   * @return String[] of arguments with which user's job class should be called
   */
  String[] extractUserArgs(String[] arguments) {
    String[] userJobArgs = new String[(arguments.length - TOTAL_JOB_WRAPPER_ARGUMENTS)];
    System.arraycopy(arguments, TOTAL_JOB_WRAPPER_ARGUMENTS, userJobArgs, 0,
                     (arguments.length - TOTAL_JOB_WRAPPER_ARGUMENTS));
    return userJobArgs;
  }

  /**
   * Function to instantiate an object of user's job class
   *
   * @param userJobClassName complete name of user's job class
   * @return a new object of user's job class
   * @throws RuntimeException if failed to instantiate an object of user's job class
   */
  Object instantiateUserJobClass(String userJobClassName) {
    Object userJobObject;
    try {
      Class<?> userJobClass = Class.forName(userJobClassName);
      userJobObject = userJobClass.newInstance();
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Unable to find the user job class: " + userJobClassName, cnfe);
      throw Throwables.propagate(cnfe);
    } catch (InstantiationException ie) {
      LOG.warn("Unable to instantiate an object of user's job class: " + userJobClassName, ie);
      throw Throwables.propagate(ie);
    } catch (IllegalAccessException iae) {
      LOG.warn("Illegal access to class: " + userJobClassName + "or to its constructor", iae);
      throw Throwables.propagate(iae);
    }
    return userJobObject;
  }

  /**
   * Function which binds the {@link SparkContextFactory} field in user's job class to a concrete implementation of
   * {@link DefaultSparkContextFactory}
   * The function checks if there is a method in user's code named as fieldname_$eq (scala's auto-generated setters) to
   * assume the user's code to be written in Scala or else in Java and binds the field accordingly.
   *
   * @param userJobObject instance of user's job class
   * @return modified user's job object in which {@link SparkContextFactory} is bind to
   * {@link DefaultSparkContextFactory}
   */
  Object sparkContextFactoryBinder(Object userJobObject) {
    for (Field field : userJobObject.getClass().getDeclaredFields()) {
      if (SparkContextFactory.class.isAssignableFrom(field.getType())) {
        if (isScalaJob(userJobObject, (field.getName() + "_$eq"))) {
          scalaJobSparkContextBinder(userJobObject, field);
        } else {
          javaJobSparkContextBinder(userJobObject, field);
        }
      }
    }
    return userJobObject;
  }

  /**
   * Function which injects {@link DefaultSparkContextFactory} to {@link SparkContextFactory} if the user's job class
   * is written in Java
   *
   * @param userJobObject instance of user's job class
   * @param field         the {@link SparkContextFactory} field
   * @throws RuntimeException if the binding to {@link DefaultSparkContextFactory} fails
   */
  private void javaJobSparkContextBinder(Object userJobObject, Field field) {
    field.setAccessible(true);
    try {
      field.set(userJobObject, new DefaultSparkContextFactory());
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access field: " + field.getName(), iae);
      throw Throwables.propagate(iae);
    }
  }

  /**
   * Function which injects {@link DefaultSparkContextFactory} to {@link SparkContextFactory} if the user's job class
   * is written in Scala
   *
   * @param userJobObject instance of user's job class
   * @param field         the {@link SparkContextFactory} field
   * @throws RuntimeException if the binding to {@link DefaultSparkContextFactory} fails
   */
  private void scalaJobSparkContextBinder(Object userJobObject, Field field) {
    try {
      Method setSparkContext = userJobObject.getClass().getMethod(field.getName() + "_$eq",
                                                                  SparkContextFactory.class);
      setSparkContext.invoke(userJobObject, new DefaultSparkContextFactory());
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find setter method for field: " + field.getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access method: " + (field.getName() + "_$eq"), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("The method: " + (field.getName() + "_$eq") + "threw an exception", ite);
      throw Throwables.propagate(ite);
    }
  }

  /**
   * Function which determines if the user's job is written in Scala or Java
   * We assume that if we find a setter method for {@link SparkContextFactory} with method name "fieldname_$eq" then
   * the user's job is in Scala or else in Java.
   *
   * @param userJobObject the user's job class instance
   * @param methodName    the method name to look for through reflection
   * @return a boolean which is true if the job is in scala
   */
  boolean isScalaJob(Object userJobObject, String methodName) {
    Method[] methods = userJobObject.getClass().getMethods();
    for (Method curMethod : methods) {
      if (methodName.equalsIgnoreCase(curMethod.getName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Function which invokes the main function on the user's job object
   *
   * @param userJobObject the user job's object
   * @param userJobArgs   String[] containing the arguments to be passed to user's job main function
   * @throws RuntimeException if failed to invoke main function on the user's job object
   */
  void invokeUserJobMain(Object userJobObject, String[] userJobArgs) {
    try {
      Method userJobMain = userJobObject.getClass().getMethod("main", String[].class);
      userJobMain.invoke(userJobObject, (Object) userJobArgs);
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find main method in user's job class: " + userJobObject.getClass().getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access main method in user's job class: " + userJobObject.getClass().getName(), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("User's job class main method threw an exception", ite);
      throw Throwables.propagate(ite);
    }
  }
}
