/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.common.options.OptionsParser;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A main class that initiate a {@link MasterEnvironment} and run a main class from the environment.
 */
public class MasterEnvironmentMain {

  private static final Logger LOG = LoggerFactory.getLogger(MasterEnvironmentMain.class);

  public static void main(String[] args) throws Exception {
    MainClassLoader classLoader = MainClassLoader.createFromContext();
    if (classLoader == null) {
      LOG.warn("Failed to create CDAP system ClassLoader. AuthEnforce annotation will not be rewritten.");
      doMain(args);
    } else {
      LOG.debug("Using {} as the system ClassLoader", classLoader);
      Thread.currentThread().setContextClassLoader(classLoader);
      Class<?> mainClass = classLoader.loadClass(MasterEnvironmentMain.class.getName());
      mainClass.getMethod("doMain", String[].class).invoke(null, new Object[]{args});
    }
  }

  /**
   * The actual main method that get invoke through reflection from the {@link #main(String[])} method.
   */
  @SuppressWarnings("unused")
  public static void doMain(String[] args) throws Exception {
    try {
      // System wide setup
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());

      // Intercept JUL loggers
      SLF4JBridgeHandler.removeHandlersForRootLogger();
      SLF4JBridgeHandler.install();

      EnvironmentOptions options = new EnvironmentOptions();
      String[] runnableArgs = OptionsParser.init(options, args, MasterEnvironmentMain.class.getSimpleName(),
                                                 ProjectInfo.getVersion().toString(), System.out)
        .toArray(new String[0]);

      String runnableClass = options.getRunnableClass();
      if (runnableClass == null) {
        throw new IllegalArgumentException("Missing runnable class name");
      }

      CConfiguration cConf = CConfiguration.create();
      SConfiguration sConf = SConfiguration.create();
      if (options.getExtraConfPath() != null) {
        cConf.addResource(new File(options.getExtraConfPath(), "cdap-site.xml").toURI().toURL());
        sConf.addResource(new File(options.getExtraConfPath(), "cdap-security.xml").toURI().toURL());
      }

      Configuration hConf = new Configuration();

      // Creates the master environment and load the MasterEnvironmentRunnable class from it.
      MasterEnvironment masterEnv = MasterEnvironments.setMasterEnvironment(
        MasterEnvironments.create(cConf, options.getEnvProvider()));
      MasterEnvironmentContext context = MasterEnvironments.createContext(cConf, hConf, masterEnv.getName());
      masterEnv.initialize(context);
      try {
        Class<?> cls = masterEnv.getClass().getClassLoader().loadClass(runnableClass);
        if (!MasterEnvironmentRunnable.class.isAssignableFrom(cls)) {
          throw new IllegalArgumentException("Runnable class " + runnableClass + " is not an instance of "
                                               + MasterEnvironmentRunnable.class);
        }

        @SuppressWarnings("unchecked")
        MasterEnvironmentRunnable runnable = masterEnv.createRunnable(context,
                                                                      (Class<? extends MasterEnvironmentRunnable>) cls);
        AtomicBoolean completed = new AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          if (!completed.get()) {
            runnable.stop();
          }
        }));
        runnable.run(runnableArgs);
        completed.set(true);
      } finally {
        masterEnv.destroy();
      }
    } catch (Exception e) {
      LOG.error("Failed to execute with arguments {}", Arrays.toString(args), e);
      throw e;
    }
  }
}
