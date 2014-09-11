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

import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.ScalaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.base.Throwables;
import org.apache.spark.network.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.Selector;

/**
 * Class which wraps around user's program class to integrate the spark program with CDAP.
 * This first command line argument to this class is the name of the user's Spark program class
 * followed by the arguments which will be passed to user's program class.
 * This Wrapper class is submitted to Spark and it does the following:
 * <ol>
 * <li>
 * Validates that there is at least {@link SparkProgramWrapper#PROGRAM_WRAPPER_ARGUMENTS_SIZE} command line arguments
 * </li>
 * <li>
 * Gets the user's program class through Spark's ExecutorURLClassLoader.
 * </li>
 * <li>
 * Sets {@link SparkContext} to concrete implementation of {@link JavaSparkContext} if user program implements {@link
 * JavaSparkProgram} or to {@link ScalaSparkContext} if user's program implements {@link ScalaSparkProgram}
 * </li>
 * <li>
 * Run user's program with extracted arguments from the argument list
 * </li>
 * </ol>
 */

public class SparkProgramWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramWrapper.class);
  private static final int PROGRAM_WRAPPER_ARGUMENTS_SIZE = 1;
  private final String[] arguments;
  private final Class userProgramClass;
  private static SparkContext sparkContext;
  private static boolean scalaProgram;

  // TODO: Get around Spark's limitation of only one SparkContext in a JVM and support multiple spark context:
  // REACTOR-950
  private static boolean sparkProgramSuccessful;
  private static boolean sparkProgramRunning;

  /**
   * Constructor
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's program class is not found
   */
  private SparkProgramWrapper(String[] args) {
    arguments = validateArgs(args);
    try {
      // get the Spark program main class with the custom classloader created by spark which has the program and
      // dependency jar.
      userProgramClass = Class.forName(arguments[0], true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Unable to find the program class: {}", arguments[0], cnfe);
      throw Throwables.propagate(cnfe);
    }
    setSparkContext();
  }

  public static void main(String[] args) {
    new SparkProgramWrapper(args).instantiateUserProgramClass();
  }

  /**
   * Validates command line arguments being passed
   * Expects at least {@link SparkProgramWrapper#PROGRAM_WRAPPER_ARGUMENTS_SIZE} command line arguments to be present
   *
   * @param arguments String[] the arguments
   * @return String[] if the command line arguments are sufficient else throws a {@link RuntimeException}
   * @throws IllegalArgumentException if the required numbers of command line arguments were not present
   */
  private String[] validateArgs(String[] arguments) {
    if (arguments.length < PROGRAM_WRAPPER_ARGUMENTS_SIZE) {
      throw new IllegalArgumentException("Insufficient number of arguments. Program class name followed by its" +
                                           " arguments (if any) should be provided");
    }
    return arguments;
  }

  /**
   * Extracts arguments belonging to the user's program class
   *
   * @return String[] of arguments with which user's program class should be called
   */
  private String[] extractUserArgs() {
    //TODO: The arguments should not be constructed in this way. We are getting it from hconf and we will just create
    // a string out of it. This will be fixed after we fix standalone distribution bug as its dependent on it.
    String[] userProgramArgs = new String[(arguments.length - PROGRAM_WRAPPER_ARGUMENTS_SIZE)];
    System.arraycopy(arguments, PROGRAM_WRAPPER_ARGUMENTS_SIZE, userProgramArgs, 0,
                     (arguments.length - PROGRAM_WRAPPER_ARGUMENTS_SIZE));
    return userProgramArgs;
  }

  /**
   * Instantiate an object of user's program class and call {@link #runUserProgram(Object)} to run it
   *
   * @throws RuntimeException if failed to instantiate an object of user's program class
   */
  private void instantiateUserProgramClass() {
    try {
      Object userProgramObject = userProgramClass.newInstance();
      runUserProgram(userProgramObject);
    } catch (InstantiationException ie) {
      LOG.warn("Unable to instantiate an object of program class: {}", arguments[0], ie);
      throw Throwables.propagate(ie);
    } catch (IllegalAccessException iae) {
      LOG.warn("Illegal access to class: {}", arguments[0] + "or to its constructor", iae);
      throw Throwables.propagate(iae);
    }
  }

  /**
   * Sets the {@link SparkContext} to {@link JavaSparkContext} or to {@link ScalaSparkContext} depending on whether
   * the user class implements {@link JavaSparkProgram} or {@link ScalaSparkProgram}
   */
  void setSparkContext() {
    if (JavaSparkProgram.class.isAssignableFrom(userProgramClass)) {
      sparkContext = new JavaSparkContext();
    } else if (ScalaSparkProgram.class.isAssignableFrom(userProgramClass)) {
      sparkContext = new ScalaSparkContext();
      setScalaProgram(true);
    } else {
      String error = "Spark program must implement either JavaSparkProgram or ScalaSparkProgram";
      throw new IllegalArgumentException(error);
    }
  }

  /**
   * Extracts arguments which belongs to user's program and then invokes the run method on the user's program object
   * with the arguments and the appropriate implementation {@link SparkContext}
   *
   * @param userProgramObject the user program's object
   * @throws RuntimeException if failed to invokeUserProgram main function on the user's program object
   */
  private void runUserProgram(Object userProgramObject) {
    String[] userprogramArgs = extractUserArgs();
    try {
      Method userProgramMain = userProgramClass.getMethod("run", String[].class, SparkContext.class);
      userProgramMain.invoke(userProgramObject, userprogramArgs, sparkContext);
    } catch (NoSuchMethodException nsme) {
      LOG.warn("Unable to find run method in program class: {}", userProgramObject.getClass().getName(), nsme);
      throw Throwables.propagate(nsme);
    } catch (IllegalAccessException iae) {
      LOG.warn("Unable to access run method in program class: {}", userProgramObject.getClass().getName(), iae);
      throw Throwables.propagate(iae);
    } catch (InvocationTargetException ite) {
      LOG.warn("Program class run method threw an exception", ite);
      throw Throwables.propagate(ite);
    }
  }

  /**
   * Stops the Spark program by calling {@link org.apache.spark.SparkContext#stop()}
   */
  public static void stopSparkProgram() {

    sparkContextStopBugFixer(); // to close the selector which causes a thread deadlock

    // Now stop the program
    if (isScalaProgram()) {
      ((org.apache.spark.SparkContext) getSparkContext().getOriginalSparkContext()).stop();
    } else {
      ((org.apache.spark.api.java.JavaSparkContext) getSparkContext().getOriginalSparkContext()).stop();
    }
  }

  /**
   * Fixes the thread deadlock issue in {@link org.apache.spark.SparkContext#stop} where the {@link Selector} field
   * in {@link ConnectionManager} waits for an interrupt. REACTOR-951
   */
  private static void sparkContextStopBugFixer() {
    ConnectionManager connectionManager = getConnectionManager(getSparkContext());
    if (!closeSelector(connectionManager)) {
      LOG.warn("Failed to get the Selector which can cause thread deadlock in SparkContext.stop()");
    }
  }

  /**
   * Gets the {@link Selector} field in the {@link ConnectionManager} and closes it which makes it come out of deadlock
   *
   * @param connectionManager : the {@link ConnectionManager} of this {@link SparkContext}
   */
  private static boolean closeSelector(ConnectionManager connectionManager) {
    // Get the selector field from the ConnectionManager and make it accessible
    boolean selectorClosed = false;
    for (Field field : connectionManager.getClass().getDeclaredFields()) {
      if (Selector.class.isAssignableFrom(field.getType())) {
        if (!field.isAccessible()) {
          field.setAccessible(true);
        }
        try {
          Selector selector = (Selector) field.get(connectionManager);
          selector.close();
          selectorClosed = true;
          break;
        } catch (IllegalAccessException iae) {
          LOG.warn("Unable to access the selector field", iae);
          throw Throwables.propagate(iae);
        } catch (IOException ioe) {
          LOG.info("Close on Selector threw IOException", ioe);
          throw Throwables.propagate(ioe);
        }
      }
    }
    return selectorClosed;
  }

  /**
   * @return {@link ConnectionManager} from the {@link SparkContext}
   */
  private static ConnectionManager getConnectionManager(SparkContext sparkContext) {
    ConnectionManager connectionManager;
    if (isScalaProgram()) {
      connectionManager = ((org.apache.spark.SparkContext) sparkContext.getOriginalSparkContext()).env()
        .blockManager().connectionManager();
    } else {
      connectionManager = ((org.apache.spark.api.java.JavaSparkContext) sparkContext.getOriginalSparkContext())
        .env().blockManager().connectionManager();
    }
    return connectionManager;
  }

  /**
   * @return {@link SparkContext}
   */
  public static SparkContext getSparkContext() {
    return sparkContext;
  }

  /**
   * @return spark program running status which is true if it is still running else false
   */
  public static boolean isSparkProgramRunning() {
    return sparkProgramRunning;
  }

  /**
   * @param sparkProgramRunning a boolean to which the sparkProgramRunning status will be set to
   */
  public static void setSparkProgramRunning(boolean sparkProgramRunning) {
    SparkProgramWrapper.sparkProgramRunning = sparkProgramRunning;
  }

  /**
   * @return spark program success status which is true if the program succeeded else false
   */
  public static boolean isSparkProgramSuccessful() {
    return sparkProgramSuccessful;
  }

  /**
   * @param sparkProgramSuccessful a boolean to which the programSuccess status will be set to
   */
  public static void setSparkProgramSuccessful(boolean sparkProgramSuccessful) {
    SparkProgramWrapper.sparkProgramSuccessful = sparkProgramSuccessful;
  }

  /**
   * @return true if user's program is in Scala or false (in case if it is in java)
   */
  private static boolean isScalaProgram() {
    return scalaProgram;
  }

  /**
   * @param scalaProgram a boolean which sets whether the user's program is in Scala or not
   */
  public static void setScalaProgram(boolean scalaProgram) {
    SparkProgramWrapper.scalaProgram = scalaProgram;
  }
}
