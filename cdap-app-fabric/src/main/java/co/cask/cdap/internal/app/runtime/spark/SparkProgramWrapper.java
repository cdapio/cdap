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

import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.ScalaSparkProgram;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkProgram;
import com.google.common.base.Throwables;
import org.apache.spark.network.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
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
  private final Class<? extends SparkProgram> userProgramClass;
  private static BasicSparkContext basicSparkContext;
  private static SparkContext sparkContext;
  private static boolean scalaProgram;

  // TODO: Get around Spark's limitation of only one SparkContext in a JVM and support multiple spark context:
  // CDAP-4
  private static boolean sparkProgramSuccessful;
  private static boolean sparkProgramRunning;

  /**
   * Constructor
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's program class is not found
   */
  @SuppressWarnings("unchecked")
  private SparkProgramWrapper(String[] args) {
    arguments = validateArgs(args);
    try {
      // Load the user class from the ProgramClassLoader
      userProgramClass = loadUserSparkClass(arguments[0]);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to load program class: {}", arguments[0], e);
      throw Throwables.propagate(e);
    }
  }

  public static void main(String[] args) {
    new SparkProgramWrapper(args).runUserProgram();
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
   * Instantiate an object of user's program class and call {@link #runUserProgram(SparkProgram)} to run it
   *
   * @throws RuntimeException if failed to instantiate an object of user's program class
   */
  private void runUserProgram() {
    sparkContext = createSparkContext();

    try {
      runUserProgram(userProgramClass.newInstance());
    } catch (InstantiationException ie) {
      LOG.warn("Unable to instantiate an object of program class: {}", arguments[0], ie);
      throw Throwables.propagate(ie);
    } catch (IllegalAccessException iae) {
      LOG.warn("Illegal access to class: {}", arguments[0] + "or to its constructor", iae);
      throw Throwables.propagate(iae);
    } finally {
      stopSparkProgram();
    }
  }

  /**
   * Sets the {@link SparkContext} to {@link JavaSparkContext} or to {@link ScalaSparkContext} depending on whether
   * the user class implements {@link JavaSparkProgram} or {@link ScalaSparkProgram}
   */
  private SparkContext createSparkContext() {
    if (JavaSparkProgram.class.isAssignableFrom(userProgramClass)) {
      return new JavaSparkContext(basicSparkContext);
    } else if (ScalaSparkProgram.class.isAssignableFrom(userProgramClass)) {
      scalaProgram = true;
      return new ScalaSparkContext(basicSparkContext);
    } else {
      String error = "Spark program must implement either JavaSparkProgram or ScalaSparkProgram";
      throw new IllegalArgumentException(error);
    }
  }

  /**
   * Extracts arguments which belongs to user's program and then invokes the run method on the user's program object
   * with the arguments and the appropriate implementation {@link SparkContext}
   *
   * @param sparkProgram the user program's object
   * @throws RuntimeException if failed to invokeUserProgram main function on the user's program object
   */
  private void runUserProgram(SparkProgram sparkProgram) {
    try {
      sparkProgram.run(sparkContext);
    } catch (Throwable t) {
      LOG.warn("Program class run method threw an exception", t);
      throw Throwables.propagate(t);
    }
  }

  private Class<? extends SparkProgram> loadUserSparkClass(String className) throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Class<?> cls = classLoader.loadClass(className);
    if (!SparkProgram.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("User class " + arguments[0] +
                                           " does not implements " + SparkProgram.class.getName());
    }
    return (Class<? extends SparkProgram>) cls;
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
   * in {@link ConnectionManager} waits for an interrupt.
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
   * Used to set the same {@link BasicSparkContext} which is inside {@link SparkRuntimeService} for accessing resources
   * like Service Discovery, Metrics collection etc.
   * @param basicSparkContext the {@link BasicSparkContext} to set from
   */
  public static void setBasicSparkContext(BasicSparkContext basicSparkContext) {
    SparkProgramWrapper.basicSparkContext = basicSparkContext;
  }

  /**
   * @return The {@link BasicSparkContext} which will be used to run the user's {@link Spark} program
   */
  public static BasicSparkContext getBasicSparkContext() {
    return basicSparkContext;
  }
}
