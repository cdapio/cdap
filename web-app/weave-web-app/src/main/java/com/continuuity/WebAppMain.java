package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 */
public class WebAppMain {
  private static final Logger LOG = LoggerFactory.getLogger(WebAppMain.class);
  private String user = System.getProperty("user.name");
  private File dir;
  private WeaveRunnerService weaveRunnerService;
  private WeaveController weaveController;
  private Configuration hConf;
  private CConfiguration cConf;


  private void usage(Options options, String message) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("WebAppMain", options);
    if (message != null && !message.isEmpty()) {
      System.out.println("Error : " + message);
    }
  }

  public int parse(String[] args) throws Exception {
    CommandLineParser parser = new GnuParser();
    Options options = new Options();

    options.addOption("u", "user", true, "User used to run application on cluster.");
    options.addOption("d", "dir", true, "Directory to be packaged and shipped to container.");

    CommandLine commandLine = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));

    if (commandLine.hasOption("user")) {
      user = commandLine.getOptionValue("user");
    }

    if (!commandLine.hasOption("dir")) {
      usage(options, "Directory to be shipped not specified.");
      return -1;
    } else {
      dir = new File(commandLine.getOptionValue("dir"));
      if (!dir.exists()) {
        usage(options, "Directory '" + dir.getAbsolutePath() + "' doesn't exist.");
        return -1;
      }
    }

    return 0;
  }

  public int init(String[] args) throws Exception {
    int parseStatus = parse(args);
    if (parseStatus != 0) {
      return parseStatus;
    }
    hConf = new Configuration();
    cConf = CConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
          bind(WeaveRunner.class).to(WeaveRunnerService.class);
        }

        @Singleton
        @Provides
        private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                     YarnConfiguration yarnConfiguration,
                                                                     LocationFactory locationFactory) {
          String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
          return new YarnWeaveRunnerService(yarnConfiguration,
                                            configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE) + zkNamespace,
                                            LocationFactories.namespace(locationFactory, "weave"));
        }
      }
    );

    weaveRunnerService = injector.getInstance(WeaveRunnerService.class);
    return 0;
  }

  public void start() {
    weaveRunnerService.startAndWait();

    // If LogSaver is already running, return handle to that instance
    Iterable<WeaveController> weaveControllers = weaveRunnerService.lookup(WebAppWeaveApplication.getName());
    Iterator<WeaveController> iterator = weaveControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", WebAppWeaveApplication.getName());
      weaveController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...",
                 WebAppWeaveApplication.getName());
        for (; iterator.hasNext(); ) {
          WeaveController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", WebAppWeaveApplication.getName());
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", WebAppWeaveApplication.getName());
      }
    } else {
      LOG.info("Starting {} application", WebAppWeaveApplication.getName());
      WeavePreparer weavePreparer =
        weaveRunnerService.prepare(new WebAppWeaveApplication(dir))
          .setUser(user)
          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));
      weaveController = weavePreparer.start();
      weaveController.addListener(new ServiceListenerAdapter() {
        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception... stopping WebCloudAppMain.", WebAppWeaveApplication.getName(), failure);
          System.exit(1);
        }
      }, MoreExecutors.sameThreadExecutor());
    }

  }

  public static void main(String[] args) throws Exception {
  }

}
