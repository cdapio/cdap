package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.common.utils.Copyright;
import com.continuuity.common.utils.UsageException;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;

/**
 * Generic wrapper class to run weave applications.
 */
public class WeaveRunnerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(WeaveRunnerMain.class);

  private WeaveRunnerService weaveRunnerService;
  private WeaveController weaveController;

  private String yarnUser;

  private String serviceName;
  private Class<?> clazz;

  public static void main(String[] args) throws Exception {
    new WeaveRunnerMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    if (args.length != 1) {
      throw usage();
    }

    serviceName = args[0];

    try {
      clazz = Class.forName(serviceName);
    } catch (ClassNotFoundException e) {
      throw usage();
    }

    if (!(WeaveApplication.class.isAssignableFrom(clazz) || WeaveRunnable.class.isAssignableFrom(clazz))) {
      throw usage();
    }

    CConfiguration cConf = CConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
        }

        @Singleton
        @Provides
        private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                     YarnConfiguration yarnConfiguration,
                                                                     LocationFactory locationFactory) {
          String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
          return new YarnWeaveRunnerService(yarnConfiguration,
                                            configuration.get(Constants.Zookeeper.QUORUM) + zkNamespace,
                                            LocationFactories.namespace(locationFactory, "weave"));
        }
      }
    );

    weaveRunnerService = injector.getInstance(WeaveRunnerService.class);
    yarnUser = cConf.get(Constants.CFG_YARN_USER, System.getProperty("user.name"));
  }

  @Override
  public void start() {
    weaveRunnerService.startAndWait();

    // If service is already running, return handle to that instance
    Iterable<WeaveController> weaveControllers = weaveRunnerService.lookup(serviceName);
    Iterator<WeaveController> iterator = weaveControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", serviceName);
      weaveController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...", serviceName);
        for (; iterator.hasNext(); ) {
          WeaveController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", serviceName);
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", serviceName);
      }
    } else {
      LOG.info("Starting {} application", serviceName);
      WeavePreparer weavePreparer = getPreparer();
      weaveController = weavePreparer.start();

      weaveController.addListener(new ServiceListenerAdapter() {
        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception... stopping LogSaverMain.", serviceName, failure);
          System.exit(1);
        }
      }, MoreExecutors.sameThreadExecutor());
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", serviceName);
    if (weaveController != null && weaveController.isRunning()) {
      weaveController.stopAndWait();
    }
  }

  @Override
  public void destroy() {
    LOG.info("Destroying {}", serviceName);
    if (weaveRunnerService != null && weaveRunnerService.isRunning()) {
      weaveRunnerService.stopAndWait();
    }
  }

  private RuntimeException usage() {
    PrintStream out = System.err;
    String name = "weave-runner";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    Copyright.print(out);
    out.println("Usage: ");
    out.println("  " + name + " <class-name>");
    out.println("  where:");
    out.println("    class-name is WeaveApplication or WeaveRunnable (fully qualified name).");

    return new UsageException();
  }

  private WeavePreparer getPreparer() {
    try {
      if (clazz.isAssignableFrom(WeaveApplication.class)) {
        WeaveApplication weaveApplication = (WeaveApplication) clazz.newInstance();
        return prepareApplication(weaveApplication);
      } else {
        WeaveRunnable weaveRunnable = (WeaveRunnable) clazz.newInstance();
        return prepareRunnable(weaveRunnable);
      }
    } catch (Exception e) {
      LOG.error("Error instantiating class {}", serviceName, e);
      throw Throwables.propagate(e);
    }
  }

  private WeavePreparer prepareRunnable(WeaveRunnable runnable) {
    return weaveRunnerService.prepare(runnable)
      .setUser(yarnUser)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));
  }

  private WeavePreparer prepareApplication(WeaveApplication application) {
    return weaveRunnerService.prepare(application)
      .setUser(yarnUser)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));
  }
}
