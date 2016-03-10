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
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Class which wraps around user's program class to integrate the spark program with CDAP.
 * The first command line argument to this class is the name of the user's Spark program class.
 */
public class SparkProgramWrapper {

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

  public void run() throws Exception {
    ExecutionSparkContext context = setupSparkContext(userSparkProgram.getClass());
    LOG.debug("Launching user spark program {} from class {}", context, userSparkProgram.getClass().getName());
    userSparkProgram.run(context);
  }

  /**
   * Setups the {@link SparkContext} for the user program. It will create the appropriate
   * {@link SparkFacade} to setup the {@link ExecutionSparkContext}.
   */
  private ExecutionSparkContext setupSparkContext(Class<? extends SparkProgram> sparkProgramClass) throws IOException {
    ExecutionSparkContext sparkContext = SparkContextProvider.getSparkContext();

    SparkConf sparkConf = new SparkConf();

    // Copy all hadoop configurations to the SparkConf, prefix with "spark.hadoop.". This is
    // how Spark YARN client get hold of Hadoop configurations if those configurations are not in classpath,
    // which is true in CM cluster due to private hadoop conf directory and YARN-4727
    Configuration hConf = sparkContext.getContextConfig().getConfiguration();
    for (Map.Entry<String, String> entry : hConf) {
      sparkConf.set("spark.hadoop." + entry.getKey(), hConf.get(entry.getKey()));
    }

    sparkConf.setAppName(sparkContext.getProgramId().getId());

    if (!sparkContext.getContextConfig().isLocal()) {
      // Create the __spark_conf.zip conf archive. It's for bug CDAP-5019 (SPARK-13441)
      Properties properties = new Properties();
      for (Tuple2<String, String> tuple : sparkConf.getAll()) {
        properties.put(tuple._1(), tuple._2());
      }

      SparkUtils.createSparkConfZip(properties);
    }

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
