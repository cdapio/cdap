/*
 * Copyright © 2020-2022 Cask Data, Inc.
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

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.app.MainClassLoader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.common.options.OptionsParser;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.master.environment.DefaultMasterEnvironmentRunnableContext;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.auth.context.SystemAuthenticationContext;
import io.cdap.cdap.security.auth.context.WorkerAuthenticationContext;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * A main class that initiate a {@link MasterEnvironment} and run a main class from the
 * environment.
 */
public class MasterEnvironmentMain {

  private static final Logger LOG = LoggerFactory.getLogger(MasterEnvironmentMain.class);
  private static TokenManager tokenManager;

  public static void main(String[] args) throws Exception {
    MainClassLoader classLoader = MainClassLoader.createFromContext();
    if (classLoader == null) {
      LOG.warn(
          "Failed to create CDAP system ClassLoader. AuthEnforce annotation will not be rewritten.");
      doMain(args);
    } else {
      LOG.debug("Using {} as the system ClassLoader", classLoader);
      Thread.currentThread().setContextClassLoader(classLoader);
      Class<?> mainClass = classLoader.loadClass(MasterEnvironmentMain.class.getName());
      mainClass.getMethod("doMain", String[].class).invoke(null, new Object[]{args});
    }
  }

  /**
   * The actual main method that get invoke through reflection from the {@link #main(String[])}
   * method.
   */
  @SuppressWarnings("unused")
  public static void doMain(String[] args) throws Exception {
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    try {
      // System wide setup
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());

      // Intercept JUL loggers
      SLF4JBridgeHandler.removeHandlersForRootLogger();
      SLF4JBridgeHandler.install();

      EnvironmentOptions options = new EnvironmentOptions();
      String[] runnableArgs = OptionsParser.init(options, args,
              MasterEnvironmentMain.class.getSimpleName(),
              ProjectInfo.getVersion().toString(), System.out)
          .toArray(new String[0]);

      String runnableClass = options.getRunnableClass();
      if (runnableClass == null) {
        throw new IllegalArgumentException("Missing runnable class name");
      }

      if (options.getExtraConfPath() != null) {
        // Copy config files from per-run configmap to the working directory
        FileUtils.copyDirectory(new File(options.getExtraConfPath()), new File("."));
      }
      CConfiguration cConf = CConfiguration.create();
      File cConfFile = new File("cConf.xml");
      if (cConfFile.exists()) {
        cConf.addResource(cConfFile.toURI().toURL());
      }
      SConfiguration sConf = SConfiguration.create();
      File sConfFile = new File("sConf.xml");
      if (sConfFile.exists()) {
        sConf.addResource(sConfFile.toURI().toURL());
      }

      SecurityUtil.loginForMasterService(cConf);

      Configuration hConf = new Configuration();
      File hConfFile = new File("hConf.xml");
      if (hConfFile.exists()) {
        hConf.addResource(hConfFile.toURI().toURL());
      }

      // Creates the master environment and load the MasterEnvironmentRunnable class from it.
      MasterEnvironment masterEnv = MasterEnvironments.setMasterEnvironment(
          MasterEnvironments.create(cConf, options.getEnvProvider()));
      MasterEnvironmentContext context = MasterEnvironments.createContext(cConf, hConf,
          masterEnv.getName());
      masterEnv.initialize(context);
      try {
        Class<?> cls = masterEnv.getClass().getClassLoader().loadClass(runnableClass);
        if (!MasterEnvironmentRunnable.class.isAssignableFrom(cls)) {
          throw new IllegalArgumentException(
              "Runnable class " + runnableClass + " is not an instance of "
                  + MasterEnvironmentRunnable.class);
        }

        RemoteClientFactory remoteClientFactory = new RemoteClientFactory(
            masterEnv.getDiscoveryServiceClientSupplier().get(),
            getInternalAuthenticator(cConf), getRemoteAuthenticator(cConf),
            cConf);

        MasterEnvironmentRunnableContext runnableContext =
            new DefaultMasterEnvironmentRunnableContext(context.getLocationFactory(),
                remoteClientFactory, cConf);
        @SuppressWarnings("unchecked")
        MasterEnvironmentRunnable runnable = masterEnv.createRunnable(runnableContext,
            (Class<? extends MasterEnvironmentRunnable>) cls);
        AtomicBoolean completed = new AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          if (!completed.get()) {
            runnable.stop();
            Uninterruptibles.awaitUninterruptibly(shutdownLatch, 30, TimeUnit.SECONDS);
          }
          Optional.ofNullable(tokenManager).ifPresent(TokenManager::stopAndWait);
        }));
        runnable.run(runnableArgs);
        completed.set(true);
      } finally {
        masterEnv.destroy();
      }
    } catch (Exception e) {
      LOG.error("Failed to execute with arguments {}", Arrays.toString(args), e);
      throw e;
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Return {@link InternalAuthenticator} with {@link SystemAuthenticationContext} if cdap-secret is
   * mounted (e.g. when only running system code / trusted code) or {@link
   * WorkerAuthenticationContext} if cdap-secret is not mounted (e.g. running untrusted user
   * provided code)
   */
  private static InternalAuthenticator getInternalAuthenticator(CConfiguration cConf) {
    File sConfFile = new File(cConf.get(Constants.Twill.Security.MASTER_SECRET_DISK_PATH));
    Injector injector;
    if (sConfFile.exists()) {
      // cdap-secret is mounted and available, use system authentication context
      injector = Guice.createInjector(
          new IOModule(),
          new ConfigModule(cConf),
          CoreSecurityRuntimeModule.getDistributedModule(cConf),
          new AuthenticationContextModules().getMasterModule());
      if (cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED)) {
        tokenManager = injector.getInstance(TokenManager.class);
        tokenManager.startAndWait();
      }
    } else {
      // cdap-secret is NOT mounted, use worker authentication context
      injector = Guice.createInjector(
          new IOModule(),
          new ConfigModule(cConf),
          CoreSecurityRuntimeModule.getDistributedModule(cConf),
          new AuthenticationContextModules().getMasterWorkerModule());
    }
    return injector.getInstance(InternalAuthenticator.class);
  }

  /**
   * Returns a new {@link RemoteAuthenticator} via injection.
   *
   * @return A new {@link RemoteAuthenticator} instance.
   */
  private static RemoteAuthenticator getRemoteAuthenticator(CConfiguration cConf) {
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        RemoteAuthenticatorModules.getDefaultModule()
    );
    return injector.getInstance(RemoteAuthenticator.class);
  }
}

