/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.api.spark.SparkProgram;
import com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which wraps around user's program class to integrate the spark program with CDAP.
 * The first command line argument to this class is the name of the user's Spark program class.
 */
public class SparkProgramWrapper implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramWrapper.class);

  private final SparkProgram userSparkProgram;

  public static void main(String[] args) throws Exception {
    SparkProgramWrapper programWrapper = new SparkProgramWrapper(args);
    programWrapper.run();
  }

  /**
   * Constructor. Starts a new {@link org.apache.spark.SparkContext} and exeuctes the user {@link SparkProgram}.
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's program class is not found
   */
  private SparkProgramWrapper(String[] args) throws Exception {
    Preconditions.checkArgument(args.length >= 1, "Missing SparkProgram class name.");

    // Load the user class from the ProgramClassLoader
    this.userSparkProgram = loadUserSparkClass(args[0]).newInstance();
  }

  @Override
  public void run() {
    ExecutionSparkContext context = setupSparkContext(userSparkProgram.getClass());
    LOG.debug("Launching user spark program {} from class {}", context, userSparkProgram.getClass().getName());
    userSparkProgram.run(context);
  }

  /**
   * Setups the {@link SparkContext} for the user program. It will create the appropriate
   * {@link SparkFacade} to setup the {@link ExecutionSparkContext}.
   */
  private ExecutionSparkContext setupSparkContext(Class<? extends SparkProgram> sparkProgramClass) {
    ExecutionSparkContext sparkContext = SparkContextProvider.getSparkContext();

    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName(sparkContext.getProgramId().getId());

    if (JavaSparkProgram.class.isAssignableFrom(sparkProgramClass)) {
      sparkContext.setSparkFacade(new JavaSparkFacade(sparkConf));
    } else if (ScalaSparkProgram.class.isAssignableFrom(sparkProgramClass)) {
      sparkContext.setSparkFacade(new ScalaSparkFacade(sparkConf));
    } else {
      String error = "Spark program must implement either JavaSparkProgram or ScalaSparkProgram";
      throw new IllegalArgumentException(error);
    }

    return sparkContext;
  }

  @SuppressWarnings("unchecked")
  private Class<? extends SparkProgram> loadUserSparkClass(String className) throws ClassNotFoundException {
    SparkClassLoader classLoader = SparkClassLoader.findFromContext();
    Class<?> cls = classLoader.getProgramClassLoader().loadClass(className);
    Preconditions.checkArgument(SparkProgram.class.isAssignableFrom(cls),
                                "User class {} does not implements {}", className, SparkProgram.class);

    return (Class<? extends SparkProgram>) cls;
  }
}
